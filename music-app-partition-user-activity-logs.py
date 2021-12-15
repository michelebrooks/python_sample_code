#1. Get logs from S3: mb-music-app-example-bucket/music-app-logs/v1 up until transition date 20180920
#   Get other logs from S3: mb-music-app-example-bucket/music-app-logs/v2
#2. Uncompress from GZIP. Delete gzip log.
#3  Add at the beggining of each row the logname, logdate, year, month, day fields.
#4. Load original and GZIP compressed to S3 using the partitions year, month, day
import sys
import os
import datetime
import time
import my_utils as mu
import gzip
import datetime
import hashlib
from functools import reduce
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
from multiprocessing import Pool
import multiprocessing
import chardet
import boto3

#---- Load Env Vars -------------
partitioned_logs_folder = os.getenv('PARTITIONED_LOGS_FOLDER')
pgdbname = os.getenv('POSTGRESS_DB_NAME')
pgdbhost = os.getenv('POSTGRESS_DB_HOST')
pgdbport = os.getenv('POSTGRESS_DB_PORT')
pgdbuser = os.getenv('POSTGRESS_DB_USER')
pgdbpwd = os.getenv('POSTGRESS_DB_PWD')

s3_partitioned_bucket = os.getenv('S3_PARTITIONED_BUCKET')
s3_partitioned_bucket_folder = os.getenv('S3_PARTITIONED_BUCKET_FOLDER')

partitions_table = os.getenv('LOAD_PARTITIONS_TABLE')
partitions_schema = os.getenv('LOAD_PARTITIONS_SCHEMA')
s3_output =  os.getenv('S3_OUTPUT')

#TODO: Check this vs the properties on the server
logging_schema =  os.getenv('POSTGRESS_LOGGING_SCHEMA')
logging_table =  os.getenv('POSTGRESS_LOGGING_TABLE')
#This will load app-logs in the current config 2018-11-21
extra_cols_prefix_key = os.getenv('DT_EXTRA_COLS_PREFIX_KEY')

def wite_partitions_to_file():
    #command = mu.get_add_partition_command('music-app','logs','mb-music-app-example-bucket/music-app-logs/v1','20170626')
    #print(command)
    #mu.write_partition_commands('2017-06-26', '2018-09-28', '/Users/mb/Documents/data_pipelines/music_app_etl/partitioned_logs/partition_commands/commands.txt')
    pass

def write_extra_cols(opened_file,log, log_buffer):
    log_name = os.path.basename(log)
    log_date_str = log_name[6:].replace('.gz', '')
    log_year = datetime.datetime.strptime(log_date_str, "%Y%m%d%H%M").year
    log_month = datetime.datetime.strptime(log_date_str, "%Y%m%d%H%M").month
    log_day = datetime.datetime.strptime(log_date_str, "%Y%m%d%H%M").day
    log_hour = datetime.datetime.strptime(log_date_str, "%Y%m%d%H%M").hour
    log_minute = datetime.datetime.strptime(log_date_str, "%Y%m%d%H%M").minute
    log_date_timestamp_gmt = int(datetime.datetime.strptime(log_date_str, "%Y%m%d%H%M").strftime("%s"))
    log_date_timestamp_gmt2 = int(datetime.datetime.strptime(log_date_str, "%Y%m%d%H%M").strftime("%s")) + 7200
    last_modified = round(time.time())
    hash_dict = dict()
    log_array = opened_file.readlines()
    #Bytes Buffer to keep the data in memory
    for i in range(len(log_array)):
        line = log_array[i]
        duplicate_val = '0'
        hash_value = hashlib.md5(line).hexdigest()
        if hash_value in hash_dict:
            duplicate_val = '1'
            hash_dict[hash_value].append(log_name+ ','+str(i+1))
        else:
            hash_dict[hash_value] = [log_name+ ','+str(i+1)]
        extra_cols = ','.join([str(last_modified), hash_value, duplicate_val, log_name, log_date_str, str(log_year), str(log_month), str(log_day), str(log_hour), str(log_minute), str(log_date_timestamp_gmt),str(log_date_timestamp_gmt2)]).encode('ascii')
        line_extra_cols = b','.join([extra_cols,line])
        log_buffer.write(line_extra_cols)

def get_log_extra_cols(log, gzipped = False):
    #I was using encoding='ISO-8859-1'
    #Extra columns to add to file
    try:
        log_buffer = BytesIO()
        if gzipped:
            with gzip.open(log, 'rb') as decompressed_file:
                write_extra_cols(decompressed_file,log,log_buffer)
                decompressed_file.close()
        else:
            with open(log, 'rb') as decompressed_file:
                write_extra_cols(decompressed_file,log,log_buffer)
                decompressed_file.close()
        #Change stream position to beginning
        log_buffer.seek(0)
        return [True,log_buffer]
    except Exception as e:
        print(e)
        return [False,log]

def process_log(log, pre_key,batch_timestamp):
    """
    This method adds to each log row custom files such as the log name, log date, year, month, date, a hash value to identify duplicates, etc
    compresses it in GZIP format and uploads it with the given key to the given S3 bucket.
    """
    print('Processing log: ', log)
    log_date = datetime.datetime.strptime(os.path.basename(log)[6:],"%Y%m%d%H%M")
    log_name = os.path.basename(log)
    log_date_ymd = str(log_date)[0:10].replace('-','')
    #key = pre_key + '/year={my_y}/month={my_m}/day={my_d}/'.format(my_y=log_date.year, my_m= '%02d' % log_date.month, my_d= '%02d' % log_date.day)+os.path.basename(log)+'.gz'
    key = pre_key + '/dt='+log_date_ymd+'/'+log_name.replace('.gz','')+'.gz'
    #GET BYTESIO logs
    success, extra_cols_buffer = get_log_extra_cols(log)
    #UPLOAD IN-MEMORY GZIP COMPRESSED FILE TO S3
    success_upload = mu.upload_gzipped(log_name, key,extra_cols_buffer)
    insert_postgres_record(log,batch_timestamp,'s3://'+s3_partitioned_bucket+'/'+key,success_upload)
    extra_cols_buffer.close()
    if not success:
        print("Parsing failed for log: ", log)
    if not success_upload:
        print("Upload unsuccessful for log: ", log)
    #if success and success_upload:
    #    print('SUCCESS Processed log: ', log)

    return success and success_upload

def get_excluded_logs_today():
    # Get Postgress connection
    mbapp_pg_conn = mu.get_postgres_db_connection(pgdbname, pgdbhost,pgdbport, pgdbuser, pgdbpwd)
    #Get all already inserted logs for TODAY
    today_date_st = datetime.datetime.now().strftime("%Y-%m-%d")
    inserted_sql = "SELECT log_dir FROM {logging_schema}.{logging_table} WHERE CAST(insert_timestamp as DATE) = CAST('{day}' AS DATE) AND upload_success = '1';".format(logging_schema=logging_schema, logging_table=logging_table, day=today_date_st)
    query_res = mu.execute_postgress_query(inserted_sql, mbapp_pg_conn,return_values= True)
    #Get a list of all to be excluded logs
    excluded_logs = [log_dir[0] for log_dir in query_res]
    mbapp_pg_conn.close()
    return excluded_logs

def load_new_partitions(schema, table_name, s3_output):
    # Get Postgress connection
    sql = "MSCK REPAIR TABLE {schema}.{table_name};".format(table_name=table_name,schema=schema)
    client = boto3.client('athena')
    response = client.start_query_execution(QueryString=sql,QueryExecutionContext={'Database': schema},
                                            ResultConfiguration={'OutputLocation': s3_output,}
                                           )
    return response

def insert_postgres_record(log_path, batch_timestamp, s3path,success_upload, add_gz = True):
    # Get Postgress connection
    mbapp_pg_conn = mu.get_postgres_db_connection(pgdbname, pgdbhost,pgdbport, pgdbuser, pgdbpwd)
    #Get all already inserted logs for TODAY
    log_n = os.path.basename(log_path) + '.gz' if add_gz else os.path.basename(log_path)
    log_date_str = log_n[6:].replace('.gz', '')
    log_date = datetime.datetime.strptime(log_date_str[:-4], '%Y%m%d')
    file_size_bytes = os.path.getsize(log_path)
    batch_date_str = datetime.datetime.fromtimestamp(batch_timestamp).strftime('%Y-%m-%d')
    sql = "INSERT INTO {logging_schema}.{logging_table} (log_name, log_date_str, log_date, s3_path, file_size_bytes, log_dir, upload_success, batch_timestamp, batch_date_str) VALUES ('{log_name}', '{log_date_str}', CAST('{log_date}' AS DATE), '{s3_path}', {file_size_bytes}, '{log_dir}','{upload_success}',to_timestamp({batch_timestamp}), '{batch_date_str}');".format(logging_schema=logging_schema, logging_table=logging_table, log_name = log_n, log_date_str = log_date_str, log_date = log_date, s3_path = s3path, file_size_bytes = file_size_bytes, log_dir = log_path, upload_success = success_upload, batch_timestamp = batch_timestamp, batch_date_str = batch_date_str)
    res = mu.execute_postgress_query(sql, mbapp_pg_conn, False)
    print(res)
    return res

def local_main(timefilter, use_timefilter = False):
    print(timefilter)
    print('Started the process')
    dirs = mu.get_dir_files_array(partitioned_logs_folder)
    dirs = [ x for x in dirs if "DS_Store" not in x ]
    if use_timefilter:
        dirs = [x for x in dirs if timefilter in x]
    dirs.sort(reverse = False)
    num_dirs_to_process = len(dirs)
    print('Processing: ', str(num_dirs_to_process), ' directories')
    excluded_logs = get_excluded_logs_today()
    for d in dirs:
        print('PROCESSING DIR: ', d)
        dir_logs = mu.get_dir_files_array(d)
        dir_logs = [ x for x in dir_logs if "mblog" in x and ".gz" in x]
        dir_logs = [ x for x in dir_logs if x not in excluded_logs]
        print('Processing: ', str(len(dir_logs)), ' logs')
        dir_logs.sort(reverse = False)
        batch_timestamp = round(time.time())
        #Multiprocessing
        p = Pool(processes= multiprocessing.cpu_count() - 1)
        batch_timestamp = time.time()
        args = [(log, extra_cols_prefix_key,batch_timestamp) for log in dir_logs]
        results = p.starmap(process_log, args)
        print(results)

        #Serial - Commented as we are multiprocessing
        #for log in dir_logs:
        #    process_log(log,extra_cols_prefix_key,batch_timestamp)

        num_dirs_to_process = num_dirs_to_process -1
        print('Num dirs to process left: ', str(num_dirs_to_process))


def main(timefilter, use_timefilter = False, exclude_prev_inserted = True):
    print(timefilter)
    print('Started the process')
    #Get excluded logs (already processed for that day)

    #Get logs in partitioned_logs_folder
    if use_timefilter:
        dir_logs = mu.get_dir_files_array(directory =partitioned_logs_folder,only_dirs=False,contains_text=True,text=timefilter)
    else:
        dir_logs = mu.get_dir_files_array(partitioned_logs_folder)
    #Only keep files that contain zalog and do not end in .gz
    dir_logs = [ x for x in dir_logs if "zalog" in x and ".gz" not in x]
    if exclude_prev_inserted:
        excluded_logs = get_excluded_logs_today()
        dir_logs = [ x for x in dir_logs if x not in excluded_logs]

    num_logs_to_process = len(dir_logs)
    print('Processing: ', str(num_logs_to_process), ' logs')
    dir_logs.sort(reverse = False)
    batch_timestamp = round(time.time())
    #Multiprocessing
    p = Pool(processes= multiprocessing.cpu_count() - 1)
    batch_timestamp = time.time()
    args = [(log, extra_cols_prefix_key,batch_timestamp) for log in dir_logs]
    results = p.starmap(process_log, args)
    print(results)

    #Serial - Commented as we are multiprocessing
    #for log in dir_logs:
    #    process_log(log,extra_cols_prefix_key,batch_timestamp)
    #    num_logs_to_process = num_logs_to_process -1
    #    print('Num dirs to process left: ', str(num_logs_to_process))

    #Load partitions if any logs were processed
    if len(dir_logs) > 0:
        start_time = time.time()
        result = load_new_partitions(partitions_schema, partitions_table,s3_output)
        print('Log partition result: ', str(result))
        print("---Loading Partitions took:  %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    len_sysargs = len(sys.argv)
    timefilter = [str((datetime.date.today() - datetime.timedelta(1)).strftime('%Y%m%d'))]
    run_with_timefilter = False
    if len_sysargs > 1:
        timefilter = sys.argv[1]
        run_with_timefilter = True
    exclude_prev_inserted = True
    if len_sysargs >= 3 and 'false' in sys.argv[2]:
        print('Excluding previously inserted logs: ')
        exclude_prev_inserted = False
        print(exclude_prev_inserted)
    print('Running for the following time filter: ', timefilter)
    main(timefilter, run_with_timefilter, exclude_prev_inserted)
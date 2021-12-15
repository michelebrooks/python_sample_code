import psycopg2
import boto3
import datetime
import time
import os

#Postgress Vars
rsdbname = os.environ['POSTGRESS_DB_NAME']
rsdbhost = os.environ['POSTGRESS_HOST']
rsdbport = os.environ['POSTGRESS_PORT']
rsdbuser = os.environ['POSTGRESS_USER']
rsdbpass = os.environ['POSTGRESS_PWD']

s3_output = 's3://mb-music-app-example/athena-results/{schema}/{table}'


def get_postgres_db_connection(rsdbname, rsdbhost, rsdbport, rsdbuser, rsdbpass):
    """
    This method creates a connection to the redshift cluster.
    """
    connection=psycopg2.connect(dbname=rsdbname, host=rsdbhost, port=rsdbport, user=rsdbuser, password=rsdbpass)
    if connection.closed:
        raise ("Could not connect to the database")
    return connection

def execute_postgress_query(query, connection, return_values = True):
    if connection is None or not connection or connection.closed != 0:
        raise ("Connection is not initialized")
    print('Running query: ', query)
    curs = connection.cursor()
    records = None
    try:
        curs.execute(query)
        if return_values:
            records = curs.fetchall()
            print('Fetched query results!')
            curs.close()
            return records
        else:
            connection.commit()
            print('Commited query!')
            curs.close()
            return True
    except Exception as e:
        print('Something went wrong when executing query: ' + query + '\n')
        print(e)
        return False

def load_single_partition(client,schema,table_name,s3_output,partition_value):
    sql = "ALTER TABLE `{schema}`.`{table_name}` ADD IF NOT EXISTS PARTITION (dt = '{dt}');".format(table_name=table_name,schema=schema,dt=partition_value)
    print(sql)
    response = client.start_query_execution(QueryString=sql,QueryExecutionContext={'Database': schema},
                                            ResultConfiguration={'OutputLocation': s3_output,}
                                           )
    return response

def check_query_execution_status(client,query_execution_id):
    """Checks the status of an Athena Query.
       If the query doesn't SUCCEED, returns FALSE, otherwise TRUE.

    Arguments:
        client {boto3.Client} -- Athena Client
        query_execution_id {string} -- query execution ID

    Returns:
        boolean -- True if query succeeded, false otherwise.
    """
    succeded = False
    while(True):
        query_exec = client.get_query_execution(**{'QueryExecutionId': query_execution_id})['QueryExecution']
        status = query_exec['Status']
        state = status['State']
        if state == 'SUCCEEDED' :
            succeded = True
            break
        if state == 'FAILED' or state == 'CANCELLED' or state == 'QUEUED':
            print(status['StateChangeReason'])
            break
        print('We sleep for 5 seconds to wait for the execution results as it is RUNNING')
        time.sleep(5)

    return succeded

def lambda_handler(event, context):
    job_flow = event['job_flow']

    print(datetime.datetime.now())
    #Dates for cluster parameters. WE PROCESS THE PREVIOUS DATE!!!!
    date = datetime.datetime.now() - datetime.timedelta(days=int(1))

    prev_date = date - datetime.timedelta(days=int(1))
    prev_date_sev = date - datetime.timedelta(days=int(7))
    prev_date_30 = date - datetime.timedelta(days=int(30))
    dt = date.strftime('%Y%m%d')
    dt_prev=prev_date.strftime('%Y%m%d')
    dt_prev_sev=prev_date_sev.strftime('%Y%m%d')
    dt_prev_30=prev_date_30.strftime('%Y%m%d')

    dt_args = {'dt':dt, 'dt_prev':dt_prev, 'dt_prev_sev':dt_prev_sev, 'dt_prev_30':dt_prev_30}

    job_id = ''
    job_group = ''
    instances={}
    applications = []
    release_label = ''
    job_flow_role= ''
    service_role=''
    steps = []
    configs =[{'Classification': 'hive-site',
            'Properties': {'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
            }
            ]

    connection = get_postgres_db_connection(rsdbname,rsdbhost,rsdbport,rsdbuser,rsdbpass)
    query = f"SELECT * FROM emr.emr_jobs WHERE name ='{job_flow}';"
    record = execute_postgress_query(query, connection, return_values = True)[0]

    job_id = record[0]
    job_group = record[2]
    applications_list = record[3].split(',')
    for application in applications_list:
        applications.append({'Name':application})
    release_label = record[4]
    job_flow_role= record[5]
    service_role=record[6]
    instances['MasterInstanceType'] = record[7]
    instances['SlaveInstanceType'] = record[8]
    instances['InstanceCount'] = record[9]
    instances['KeepJobFlowAliveWhenNoSteps'] = record[10]
    instances['TerminationProtected'] = record[11]
    instances['Ec2KeyName'] = record[12]
    #Getting Steps Data
    query = f'SELECT * FROM emr.emr_steps WHERE job_id = {job_id} ORDER BY "order" ASC;'
    records = execute_postgress_query(query, connection, return_values = True)
    for record in records:
        if record[8]:
            partition_schema = record[11]
            partition_table = record[12]
            s3_outpt = s3_output.format(schema=partition_schema,table=partition_table)
            client = boto3.client('athena')
            response = load_single_partition(client,partition_schema,partition_table,s3_outpt,dt)
            print(f'Loaded date partition for {partition_schema}.{partition_table}')
        step = {}
        step['Name'] = record[3] +  f' {dt}'
        step['ActionOnFailure'] = record[4]

        hadoop_jar_step = {}
        hadoop_jar_step['Jar'] = 'command-runner.jar'

        args_list = []
        #script type
        args_list.append(record[5])
        args_list.append('--run-hive-script')
        args_list.append('--args')
        args_list.append('-f')
        #script location
        args_list.append(record[6])
        args_list.append('-d')
        #Output
        args_list.append(record[7])
        args = record[9]
        if args:
            for arg in args.split(','):
                args_list.append('-d')
                args_list.append(arg)
        use_dt_args = record[10]
        if use_dt_args:
            for arg_k in dt_args:
                args_list.append('-d')
                arg_v = dt_args[arg_k]
                arg_f = f'{arg_k}={arg_v}'
                args_list.append(arg_f)
        hadoop_jar_step['Args'] = args_list
        step['HadoopJarStep'] = hadoop_jar_step

        steps.append(step)

    #EMR client
    client = boto3.client('emr')
    response = client.run_job_flow(
    Name=job_flow + ' ' + dt,
    LogUri='s3://mb-music-app-example-emr/logging/{group}/{job}/{dt}'.format(group=job_group,job=job_flow, dt=dt),
    ReleaseLabel=release_label,
    Instances=instances,
    JobFlowRole=job_flow_role,
    ServiceRole=service_role,
    Steps=steps,
    Applications=applications,
    Configurations = configs
    )

    print(response)
    return response
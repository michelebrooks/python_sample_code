from ftplib import FTP_TLS
from dateutil import parser
import datetime as dt
import boto3
import gzip
from io import BytesIO
import sys
import os
import json

ftp_host = '0.0.0.0'
ftp_user = 'cms_user'
ftp_pwd = 'cms_password'
ftp_port = 53000

files_path = '/home/mb_music_app_developer/albums_and_tracks/'

albums_file_name = 'albums_data.gz'
tracks_file_name = 'tracks_data.gz'


s3_output_bucket = 'music-app-bucket'
albums_s3_key = 'mb-music-app/cms/albums-json/'
tracks_s3_key = 'mb-music-app-logs/music-json/'

albums_flat_s3_key = 'mb-music-app/cms//albums-flat/'
tracks_flat_s3_key = 'mb-music-app/cms/music-flat/'

def compress_file(log_name, fp):
    """Compress and upload the contents from fp to S3.
       fp needs to be a BytesIO object
    """
    try:
        buffer = BytesIO()
        log_name_mod = log_name +'.gz' if '.gz' not in log_name else log_name
        writer = gzip.GzipFile(log_name_mod, 'wb', 6, buffer)
        writer.write(fp.getvalue())
        writer.close()
        buffer.seek(0)
        return buffer
    except Exception as e:
        print(e)
        sys.exit()

def upload_file_to_s3(file, key, s3_bucket_name = None, content_type='text/plain'):
    """
    Uploads a compressed file to AWS S3
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket_name)
    bucket.upload_fileobj(file,key,{'ContentType': content_type, 'ContentEncoding': 'gzip'})
    print('Uploaded File: {key}'.format(key=key))

def compress_and_upload(file_path, file_name, s3_output_bucket, s3_key):
    with open(file_path, 'rb') as fin:
        data = BytesIO(fin.read())
        compressed_buffer = compress_file(file_name,data)
        upload_file_to_s3(compressed_buffer,s3_key,s3_output_bucket)

def add_artists_fields(row):
    if 'artist_list' in row:
        artist_list = row['artist_list']
        artist_id_list = []
        artist_name_list = []
        for i, artist in enumerate(artist_list):
            ar_id = f'artist_id_{i}'
            ar_name= f'artist_name_{i}'
            row[ar_id] = artist['id']
            row[ar_name] = artist['name']
            artist_id_list.append(str(artist['id']))
            artist_name_list.append(str(artist['name']))
        row['artist_id_list'] = '|'.join(artist_id_list)
        row['artist_name_list'] = '|'.join(artist_name_list)
        return row

def compress_and_upload_flattened(file_path, file_name, s3_output_bucket, s3_key):
    output_path = file_path.replace('.txt','_m')+'.txt'
    with open(output_path,'wb') as mod_f:
        with open(file_path, 'r', encoding='utf-8') as fin:
            for row in fin:
                try:
                   row_m = add_artists_fields(json.loads(row))
                   mod_f.write(json.dumps(row_m).encode('utf-8')+ b'\n')
                except:
                    print('Error: ',sys.exc_info()[0], ' ignoring row')
                    pass

    with open(output_path, 'rb') as fin:
        data = BytesIO(fin.read())
        compressed_buffer = compress_file(file_name,data)
        upload_file_to_s3(compressed_buffer,s3_key,s3_output_bucket)
        print('Removing file: ', output_path)
        os.remove(output_path)


ftp = FTP_TLS()

#Connecting
ftp=FTP_TLS()
ftp.set_debuglevel(2)
#Authenticating
ftp.connect(ftp_host, ftp_port)
ftp.sendcmd('USER {user}'.format(user=ftp_user))
ftp.sendcmd('PASS {pwd}'.format(pwd=ftp_pwd))

lines = []
ftp.dir("/", lines.append)

# [file, timestamp]
max_album = [None, dt.datetime(1999, 1, 1)]
max_track = [None, dt.datetime(1999, 1, 1)]

for line in lines:
    tokens = line.split(maxsplit = 9)
    name = tokens[8]
    time_str = tokens[5] + " " + tokens[6] + " " + tokens[7]
    time = parser.parse(time_str)
    #print(name + ' - ' + str(time))
    if 'track' in name and max_track[1] < time:
        max_track = [name, time]
    if 'album' in name and max_album[1] < time:
        max_album = [name, time]
print('Max album: ')
print(max_album)
print('Max track: ')
print(max_track)

#Download albums
handle = open(max_album[0].lstrip('/'), 'wb')
ftp.retrbinary('RETR %s' % max_album[0], handle.write)
#Compress to GZIP and upload albums to s3
albums_file_path = files_path + max_album[0]
print('Processing AlbumsJson')
albums_key = albums_s3_key + albums_file_name
compress_and_upload(albums_file_path,albums_file_name,s3_output_bucket,albums_key)
print('Processing AlbumsFlat')
albums_key = albums_flat_s3_key + albums_file_name
compress_and_upload_flattened(albums_file_path,albums_file_name,s3_output_bucket,albums_key)

#Download tracks
handle = open(max_track[0].lstrip('/'), 'wb')
ftp.retrbinary('RETR %s' % max_track[0], handle.write)
#Compress to GZIP and upload tracks to s3
print('Processing SongsJson')
tracks_file_path = files_path + max_track[0]
tracks_key = tracks_s3_key + tracks_file_name
compress_and_upload(tracks_file_path,tracks_file_name,s3_output_bucket,tracks_key)
print('Processing SongsFlat')
tracks_key = tracks_flat_s3_key + tracks_file_name
compress_and_upload_flattened(tracks_file_path,tracks_file_name,s3_output_bucket,tracks_key)

#Close FTP service
ftp.close()

#Delete albums file
os.remove(albums_file_path)
print('File {file} Removed!'.format(file=albums_file_path))
#Delete tracks file
os.remove(tracks_file_path)
print('File {file} Removed!'.format(file=tracks_file_path))
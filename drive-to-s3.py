

from googleapiclient import discovery
from google.oauth2 import service_account
import json
from apiclient.http import MediaIoBaseDownload

import boto3
import os
import io
from io import StringIO 

import zipfile

import boto3
from botocore.exceptions import NoCredentialsError

ACCESS_KEY = #access key of aws
SECRET_KEY = #secret key of aws

    
def get_service_account_credentials():
    from google.oauth2 import service_account
    
    creds_dict = #json file of service account of gcp
    creds_json = json.loads(creds_dict, strict=False)

    scopes_list = [

        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file'
    ]
    credentials = service_account.Credentials.from_service_account_info(creds_json, scopes=scopes_list)         
    return credentials
    
    
    
def lambda_handler(event, context):


    credentials = get_service_account_credentials()
   
    service = discovery.build('drive', 'v3',credentials=credentials, cache_discovery=False)
   
    file_ids = ['1p6ZnevK4_77LmvJEn0nONvBvQPsIVHrt']
    file_names = ['harsh67.jpg']
    
    
    #zip return an iterator of tuple
    
    for file_id,file_name in zip(file_ids,file_names):
        
        request = service.files().get_media(fileId=file_id)
       
        fh = io.BytesIO()
        
        chunksize = 5 * 1024 * 1024
        downloader = MediaIoBaseDownload(fd = fh, request=request,chunksize=chunksize)
        done= False
         
        while done is False:
            try:
                status, done = downloader.next_chunk()
                print(downloader)
                print('Download progres {0}'.format(status.progress()*100))
            except Exception as error:
                print("Error: {}".format(error))
                fh.close()
            
    
    
        with open(os.path.join('/tmp',file_name),'wb') as f:
   
            fh.seek(0)
            h= f.write(fh.read())
            fh.truncate(0)
            print(h)
            print(fh)
            fh.close()
            
           
            #aws code
            
            
            
        # local_file = "/tmp/{0}".format(file_name)
        local_file = "/tmp/{0}".format(file_name)
        bucket = 'sql-server-shack-demo-6'
        s3_file =  file_name
            
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY)
        
        try:
            s3.upload_file(local_file, bucket, s3_file)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False
                
                
        # fh.close()
                
    
   
    
        

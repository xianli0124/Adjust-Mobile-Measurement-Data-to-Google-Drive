import numpy as np
import pandas as pd
import boto3
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

"""
network: advertising network
start_date: str - YYYY-MM-DD
end_date: str - YYYY-MM-DD
access_key: access key for AWS bucket where the Google Auth token is stored
secret_key: secret key for AWS bucket where the Google Auth token is stored
bucket_name: AWS bucket where the Google Auth token is stored
token_name: name of the Google Auth token in the AWS bucket
folder_id: folder id on Google Drive to upload the report
"""

class Google_Sheets_Connection:
    start_date = None
    end_date = None
    network = None
    folder_id = None
    file_list = None
    drive = None

    def __init__(self, network, start_date, end_date, access_key, secret_key, bucket_name, token_name, folder_id):
        self.network = network
        self.start_date = start_date
        self.end_date = end_date
        self.folder_id = folder_id
        
        s3 = boto3.client(
            's3',
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key
        )
        
        with open('/tmp/mycreds.txt', 'wb') as f:
            s3.download_fileobj(bucket_name, token_name, f)

        gauth = GoogleAuth()
        gauth.LoadCredentialsFile('/tmp/mycreds.txt')
        if gauth.credentials is None:
            # Authenticate if they're not there
            gauth.LocalWebserverAuth()
        elif gauth.access_token_expired:
            # Refresh them if expired
            gauth.Refresh()
        else:
            # Initialize the saved creds
            gauth.Authorize()

        drive = GoogleDrive(gauth)
        self.drive = drive
    
    def upload(self):
        fileID = self.network + ' ' + self.start_date + '_' + self.end_date + '.xlsx'
        file = self.drive.CreateFile({'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
                                       'title': fileID, 
                                       'parents': [{'id': self.folder_id}]})
        file.SetContentFile('/tmp/' + fileID)
        file.Upload() 
        
    def get_folder_content(self):
        file_list = self.drive.ListFile({'q': "'" + self.folder_id + "' in parents and trashed=false"}).GetList()
        files = {i['title'] : i['id'] for i in file_list}
        self.file_list = files
        
    '''
    Files must be named "<network> <start_date>_<end_date>.xlsx"
    '''
    def delete_old_files(self):
        keys = self.file_list.keys()
        delete_file_list = []
        cutoff = pd.to_datetime(self.start_date) - pd.Timedelta('14d')) 
        for i in keys:
            date_str = i.split(' ')[1]
            date_str = date_str.split('_')[0]
            if pd.to_datetime(date_str) <= cutoff:
                delete_file_list.append(i)
        if len(delete_file_list) > 0:
            for i in delete_file_list:
                file_to_delete = self.drive.CreateFile({'id': self.file_list[i]})
                file_to_delete.Delete()
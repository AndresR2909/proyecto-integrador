from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
import logging
import os

logging.basicConfig(level=logging.ERROR) 

connection_string = os.environ.get('DATALAKE_CONNECTION_STRING')



class DataLakeStorage:
    def __init__(self,zone:str) -> None:
        try:
            self.dl_client = FileSystemClient.from_connection_string(connection_string, 
                                                                  zone)

        except Exception as e:
            logging.error(f"Error connecting to blob storage: {e}")
    
    def list_files_from_path(self,directory_name: str):
        try:                    
            paths = self.dl_client.get_paths(path=directory_name)
            file_list = [{'filename': f.name, 'lastupdate':f.last_modified } for f in paths] 
            return file_list
        except Exception as e:
            print(f"error: {str(e)}, message: error al listar archivos")
            return None

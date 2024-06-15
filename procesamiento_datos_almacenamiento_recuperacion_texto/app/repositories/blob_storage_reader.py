from app.interfaces.data_reader import IDataReader
from external.blob_storage import BlobStorage  
import pandas as pd
from datetime import datetime
  
class BlobStorageReader(IDataReader):  
    def __init__(self, storage: BlobStorage):  
        self.storage = storage  
  
    def read(self, source: str):  
        data = self.storage.download_file_from_blob(source)  
        return data  
    
    def list_files(self, source: str):  
        files = self.storage.list_files_from_container()
        file_list = [f['filename'] for f in files]  
        return file_list  
    
    def read_full(self):  
        files = self.storage.list_files_from_container()
        file_list = [f['filename'] for f in files]
        lista_dataframes = []

        # Iterar sobre los archivos, leer cada archivo y agregar su DataFrame a la lista
        for filename in file_list:
            file =self.storage.download_file_from_blob(filename)
            df = pd.read_csv(file,sep=";")
            lista_dataframes.append(df)  
        df_full = pd.concat(lista_dataframes, ignore_index=True)
        return df_full  
    
    def read_delta(self, filter_date:datetime):  
        files = self.storage.list_files_from_container()
        file_list = self._filter_files(files, filter_date)
        lista_dataframes = []
        # Iterar sobre los archivos, leer cada archivo y agregar su DataFrame a la lista
        for filename in file_list:
            file =self.storage.download_file_from_blob(filename)
            df = pd.read_csv(file,sep=";")
            lista_dataframes.append(df)  
        if len(lista_dataframes)>0:
            df_delta = pd.concat(lista_dataframes, ignore_index=True)
        else:
            df_delta = pd.DataFrame([])

        return df_delta    


    def _filter_files(self,files_dict_list:list[dict], filter_date:datetime)->list[str]:
        if filter_date:
            filtered_files = [ file['filename'] for file in files_dict_list if  file['lastupdate']>=filter_date]
        else:
            filtered_files  = [file['filename'] for file in files_dict_list]
        return filtered_files
from app.interfaces.data_reader import IDataLakeReader 
from external.data_lake_pandas import DataLakePandas
from external.data_lake_storage import DataLakeStorage
import pandas as pd  
from datetime import datetime
  
class DataLakeReader(IDataLakeReader):  
    def __init__(self, storage_pandas: DataLakePandas, storage: DataLakeStorage):  
        self.storage_pandas = storage_pandas  
        self.storage = storage  
  
    def read(self, source: str, file_name:str)->pd.DataFrame:  
        df = self.storage_pandas.read_dataframe_from_parquet(container_name=source,
                                               file_name=file_name,
                                               filter=None) 
        return df
    
    def read_csv(self, source: str, file_name:str, sep:str=";")->pd.DataFrame:  
        df = self.storage_pandas.read_dataframe_from_csv(container_name=source,
                                               file_name=file_name,
                                               sep=sep) 
        return df
    
    
    def read_parquet(self, source:str, file_name:str, filter)->pd.DataFrame:  

        df = self.storage_pandas.read_dataframe_from_parquet(container_name=source,
                                        file_name=file_name,
                                        filter=filter) 
        return df  


    def read_full(self, source:str, path_name:str,  filter)->pd.DataFrame: 
        files = self.storage.list_files_from_path(path_name)
        file_list = [f['filename'] for f in files]
        lista_dataframes = []
        
        # Iterar sobre los archivos, leer cada archivo y agregar su DataFrame a la lista
        for filename in file_list:
            print(filename)
            df = self.storage_pandas.read_dataframe_from_parquet(container_name=source,
                                        file_name=filename,
                                        filter=filter) 
            lista_dataframes.append(df)  
        df_full = pd.concat(lista_dataframes, ignore_index=True)
        return df_full 


    def read_delta(self, source:str, path_name:str, filter_date: datetime, filter)->pd.DataFrame:
        files = self.storage.list_files_from_path(path_name)
        file_list = self._filter_files(files, filter_date)
        lista_dataframes = []

                # Iterar sobre los archivos, leer cada archivo y agregar su DataFrame a la lista
        for filename in file_list:
            df = self.storage_pandas.read_dataframe_from_parquet(container_name=source,
                                        file_name=filename,
                                        filter=filter) 
            lista_dataframes.append(df)  
        if len(lista_dataframes)>0:
            df_delta = pd.concat(lista_dataframes, ignore_index=True)
        else:
            df_delta = None

        return df_delta   


    def _filter_files(self,files_dict_list:list[dict], filter_date:datetime)->list[str]:
        if filter_date:
            filtered_files = [ file['filename'] for file in files_dict_list if  file['lastupdate']>=filter_date]
        else:
            filtered_files  = [file['filename'] for file in files_dict_list]
        return filtered_files
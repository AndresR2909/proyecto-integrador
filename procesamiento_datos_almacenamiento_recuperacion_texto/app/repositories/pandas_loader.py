from app.interfaces.data_loader import IDataLakeLoader 
from external.data_lake_pandas import DataLakePandas
import pandas as pd  
  
class DataLakeLoader(IDataLakeLoader):  
    def __init__(self, storage: DataLakePandas):  
        self.storage = storage  
  
    def write_parquet(self, df: pd.DataFrame, container_name:str,file_name:str, partition_col:str = 'year')->None:  
        self.storage.save_dataframe_to_parquet(df,
                                               container_name=container_name,
                                               file_name=file_name,
                                               partition_col=partition_col)
    def write_parquet_one_file(self, df: pd.DataFrame, container_name:str,file_name:str, partition_col:str = 'year')->None:  
        self.storage.save_dataframe_to_parquet_one_file(df,
                                               container_name=container_name,
                                               file_name=file_name,
                                               partition_col=partition_col)
    def write_csv(self, df: pd.DataFrame, container_name:str,file_name:str, sep:str=';')->None:  
        self.storage.save_dataframe_to_csv(df,
                                               container_name=container_name,
                                               file_name=file_name,
                                               sep=sep)
        
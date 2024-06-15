from app.interfaces.data_loader import IDataLakeLoader 
from external.data_lake_pandas import DataLakePandas , pd
  
class BlobStorageLoader(IDataLakeLoader):  
    def __init__(self, storage: DataLakePandas):  
        self.storage = storage  
  
    def write_parquet(self, df: pd.DataFrame, container_name:str,file_name:str, partition_col:str)->None:
        self.storage.save_dataframe_to_parquet(df, 
                                   container_name,
                                   file_name, 
                                   partition_col)
    
    def write_csv(self, df: pd.DataFrame, container_name:str,file_name:str, sep:str=';')->None:
        self.storage.save_dataframe_to_csv(df,
                                        container_name,
                                        file_name, 
                                        sep)
from abc import ABC, abstractmethod  
import pandas as pd
  
class IDataLoader(ABC):  
    @abstractmethod  
    def write(self, destination: str, filename:str, data):  
        pass

class IDataLakeLoader(ABC):  
    
    @abstractmethod   
    def write_csv(self, df: pd.DataFrame, container_name:str,file_name:str, sep:str=';'): 
        pass 

    @abstractmethod 
    def write_parquet(self, df: pd.DataFrame, container_name:str,file_name:str, partition_col:str):  
        pass  
    @abstractmethod 
    def write_parquet_one_file(self, df: pd.DataFrame, container_name:str,file_name:str, partition_col:str):
        pass
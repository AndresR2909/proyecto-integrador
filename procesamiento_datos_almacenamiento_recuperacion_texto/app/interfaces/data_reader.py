from abc import ABC, abstractmethod  
from datetime import datetime
import pandas as pd
  
class IDataReader(ABC):  
    @abstractmethod  
    def read(self, source: str):  
        pass  
    @abstractmethod
    def read_full(self)->pd.DataFrame: 
        pass  
    @abstractmethod
    def read_delta(self, delta: datetime)->pd.DataFrame:  
        pass  

class IDataLakeReader(ABC):  
    @abstractmethod  
    def read(self, source: str, file_name:str)->pd.DataFrame:  
        pass  
    @abstractmethod
    def read_csv(self, source: str, file_name:str, sep:str=";")->pd.DataFrame: 
        pass 
    @abstractmethod 
    def read_parquet(self, source:str, file_name:str, filter)->pd.DataFrame:  
        pass 
    @abstractmethod
    def read_delta(self, source:str, path_name:str, filter_date: datetime, filter)->pd.DataFrame: 
        pass
    @abstractmethod
    def read_full(self, source:str, path_name:str,  filter)->pd.DataFrame:
        pass

class IDataLakeManager(ABC):  
    @abstractmethod  
    def list_files(self, source: str, file_name:str)->pd.DataFrame:  
        pass 
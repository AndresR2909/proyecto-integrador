from abc import ABC, abstractmethod  

class ITableDataReader(ABC):  
    
    @abstractmethod   
    def query_table(self, query:str, table_name:str): 
        pass 
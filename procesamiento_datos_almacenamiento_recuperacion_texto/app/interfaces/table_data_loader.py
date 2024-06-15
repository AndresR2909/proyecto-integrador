from abc import ABC, abstractmethod  

class ITableDataLoader(ABC):  
    
    @abstractmethod   
    def load(self, entities_list:list, table_name:str): 
        pass 
from abc import ABC, abstractmethod  
import pandas as pd
class IVdbDocumentLoader(ABC):  
    @abstractmethod 
    def load_documents_vdb(self, index_name: str, docs:list,ids:list)->tuple[list,list]:  
        pass

    @abstractmethod
    def split_documents(self,documents:list,chunk_size:int,chunk_overlap:int,token:bool)->list:  
       pass
        
    @abstractmethod
    def create_documents_from_df(self,df:pd.DataFrame)->list:  
        pass
    
    @abstractmethod
    def create_ids_documents(self,documents:list,id_column:str)->tuple[list,list]:
        pass

    @abstractmethod
    def create_index_vdb(self,index_name:str,metric:str,dimension:int,metadata_index:str)->None:  
        pass                   

    @abstractmethod
    def delete_index_vdb(self,index_name:str)->bool:  
        pass

    @abstractmethod
    def list_indexes_vdb(self)->list:  
        pass
    
    @abstractmethod
    def describe_index_vdb(self,index_name:str)->dict:  
        pass
from abc import ABC, abstractmethod  
  
class IVdbDocumentRetrieval(ABC):  

    @abstractmethod 
    def query_ids_vdb(self,id:list , index_name: str ,top:int = 5)->list:  
        pass
    
    @abstractmethod
    def query_documents_vdb(self,index_name:str,query:str,n_documents:int=4,metadata_filter:dict=None)->None:  
        pass
    
    @abstractmethod
    def list_indexes_vdb(self)->list:  
        pass
    
    def describe_index_vdb(self,index_name)->dict:  
        pass
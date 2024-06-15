from abc import ABC, abstractmethod  
class IAgentManager(ABC):  
    @abstractmethod  
    def summary_documents(self, documents:list,summary_type:str)->str:  
        pass
    @abstractmethod  
    def count_tokens(self,documents:list):
        pass
    @abstractmethod 
    def count_tokens_from_string(self,documents:str):
        pass
    @abstractmethod 
    def fitted_summary_documents(self, documents:list)->str:
        pass
    @abstractmethod 
    def map_reduce_custom_summary_documents(self, document:str)->str:
        pass

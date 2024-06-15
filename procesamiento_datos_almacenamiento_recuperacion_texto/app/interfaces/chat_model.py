from abc import ABC, abstractmethod  
class IChatmodel(ABC):  
    @abstractmethod  
    def chat(self, query)->str:  
        pass
    @abstractmethod 
    def chat_with_context(self, query, context)->str:  
        pass
    @abstractmethod 
    def chat_with_memory(self, query, memory)->str:  
        pass
    @abstractmethod 
    def chat_with_context_memory(self, context, memory)->str:  
        pass

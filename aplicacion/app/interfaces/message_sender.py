from abc import ABC, abstractmethod  

class IMessageSender(ABC):  
    
    @abstractmethod   
    def send_message(self, msn:str, receiver:str): 
        pass 
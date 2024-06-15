from app.interfaces.agent_manager import IAgentManager
from external.lang_chain_manager import LangChainManager
from external.open_ia_chat import OpenAIChatModel
import logging

class AgentManagerRepository(IAgentManager):
    """PinceconeVDB repository."""

    def __init__(self, llm: OpenAIChatModel, manager:LangChainManager):
        self._llm = llm
        self._manager = manager

    def summary_documents(self, documents:list,summary_type:str="stuff")->str:  
        if summary_type == "refine":
            resume = self._manager.resume_documents_refine(documents,self._llm.chat_model_client)
        elif summary_type == "stuff":
            resume = self._manager.resume_documents_stuff(documents,self._llm.chat_model_client)
        elif summary_type == "map_reduce":
            resume = self._manager.resume_documents_with_map_reduce(documents,self._llm.chat_model_client)
        else:
            msn=f"Parameter {summary_type} no valid, chose among 'refine', 'stuff' or 'map_reduce'"
            logging.info(msn)
            print(msn)
            resume= None

        return resume
    def fitted_summary_documents(self, documents:list)->str:  
        
        resume = self._manager.custom_summary_documents(documents,self._llm.chat_model_client)

        return resume
    
    def map_reduce_custom_summary_documents(self, document:str)->str:  
        
        map_reduce_resume = self._manager.map_reduce_custom_summary_documents(document,self._llm.chat_model_client)

        return map_reduce_resume
    
    def count_tokens(self,documents:list):
        return self._manager.count_tokens(documents)
    
    def count_tokens_from_string(self,document:str):
        return self._manager.count_tokens_from_string(document)



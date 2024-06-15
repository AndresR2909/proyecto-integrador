from app.interfaces.document_retrieval import IVdbDocumentRetrieval
from external.lang_chain_manager import LangChainManager
from external.pinecone_vdb import PineconeVectorDB

import pandas as pd

class PinceconeVDBRepository(IVdbDocumentRetrieval):
    """PinceconeVDB repository."""

    def __init__(self, vector_db: PineconeVectorDB, manager:LangChainManager):
        self._vector_db = vector_db
        self._manager = manager

    def query_ids_vdb(self,ids:list , index_name: str )->list:  
        retrieval_docs = []
        for id in ids:
            retrieval_doc = self._vector_db.document_search_by_id(id,index_name,top=1)
            retrieval_docs.extend(retrieval_doc)
        return retrieval_docs
    
    
    def query_documents_vdb(self,index_name:str,query:str,n_documents:int=4,metadata_filter:dict=None)->None:  
        docs = self._manager.document_search(index_name,query,n_documents,metadata_filter)
        return docs

    def list_indexes_vdb(self)->list:  
        return self._vector_db.list_indexes()
    
    def describe_index_vdb(self,index_name)->dict:  
        return self._vector_db.describe_index(index_name)
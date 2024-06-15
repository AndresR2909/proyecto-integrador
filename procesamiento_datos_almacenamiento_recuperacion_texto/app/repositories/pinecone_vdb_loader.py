from app.interfaces.document_loader import IVdbDocumentLoader
from external.lang_chain_manager import LangChainManager
from external.pinecone_vdb import PineconeVectorDB

import pandas as pd

class PinceconeVDBRepository(IVdbDocumentLoader):
    """PinceconeVDB repository."""

    def __init__(self, vector_db: PineconeVectorDB, manager:LangChainManager):
        self._vector_db = vector_db
        self._manager = manager

    def load_documents_vdb(self, index_name: str, docs:list,ids:list )->None:  
    
        id_list = self._manager.add_documents_vectostore(index_name,docs,ids)
        return docs,id_list

    def create_ids_documents(self,documents:list,id_column:str="video_id"):
        docs,ids = self._manager.add_ids_to_documents(documents,id_column)
        return docs,ids

    def split_documents(self,documents,chunk_size:int=1000,chunk_overlap:int=100,token:bool=True)->list:  
        return self._manager.split_document(documents, chunk_size,chunk_overlap, token=token, character=not(token))
        

    def create_documents_from_df(self,df:pd.DataFrame)->list:  
        return self._manager.load_df_document(df=df,text_content_column="text")


    def create_index_vdb(self,index_name,metric='cosine',dimension=1536,metadata_index=NotImplemented)->None:  
        self._vector_db.add_index(index_name,metric=metric , 
                               dimension= dimension,
                             metadata_index= metadata_index)


    def delete_index_vdb(self,index_name)->bool:  
        return self._vector_db.delete_index(index_name)


    def list_indexes_vdb(self)->list:  
        return self._vector_db.list_indexes()
    
    def describe_index_vdb(self,index_name)->dict:  
        return self._vector_db.describe_index(index_name)
        

            

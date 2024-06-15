from pinecone import Pinecone,ServerlessSpec, PodSpec
import time
import os
import logging

api_key = os.environ.get('PINECONE_API_KEY')
region = os.environ.get('REGION')
cloud = os.environ.get('CLOUD')
environment = os.environ.get('PINECONE_ENVIRONMENT')
use_serverless = os.environ.get("USE_SERVERLESS", "True").lower() == "true"

class PineconeVectorDB:
    """manager for Pinecone client."""

    def __init__(self):
        self._pincone_vdb_client = Pinecone(api_key=api_key)
        if use_serverless:
            spec = ServerlessSpec(cloud=cloud, region=region)
        else:
            spec = PodSpec(environment=environment,pod_type="s1.x1")
        self._spec = spec

    def document_search_by_vector(self,vector , index_name: str ,top:int = 5, filter_metadata:dict= None)->list:
        try:
            index = self._pincone_vdb_client.Index(index_name)
            top_documents = index.query(vector=vector, top_k=top, include_metadata=True, filter = filter_metadata)
        except Exception as e:
            print(e)
        return top_documents['matches']
    
    def document_search_by_id(self,id:list , index_name: str ,top:int = 5)->list:
        try:
            index = self._pincone_vdb_client.Index(index_name)
            top_documents = index.query(id=id, top_k=top, include_metadata=True, include_values=True)
        except Exception as e:
            print(e)
        return top_documents['matches']
        
    def load_dataframe_by_batch(self,index_name:str, df, batch= 100) -> None:
        """Add documents to the Pinecone vector database index in a single load."""
        try:
            index = self._pincone_vdb_client.Index(index_name)
            index.upsert_from_dataframe(df, batch_size=batch)
        except Exception as e:
            print(e)

    def describe_index(self,index_name:str) -> None:
        """Describe index into vector data base"""
        try:
            index = self._pincone_vdb_client.Index(index_name)
            index_stats = index.describe_index_stats()
        except Exception as e:
            index_stats = None
            logging.info(e)
        return index_stats

    def add_index(self, index_name: str, metric: str ='cosine', dimension = 1536, metadata_index:str='publish_date') -> None:
        """Adds intex to vector DB"""
        if index_name in self._pincone_vdb_client.list_indexes().names():
            self._pincone_vdb_client.delete_index(index_name)

        # we create a new index
        self._pincone_vdb_client.create_index(index_name,
                                            dimension=dimension,  # dimensionality of text-embedding-ada-002
                                            metric= metric, #'dotproduct'
                                            spec=self._spec)
                                            #metadata_config = {
                                            #"indexed": [metadata_index]
                                            #})

        # wait for index to be initialized
        while not self._pincone_vdb_client.describe_index(index_name).status['ready']:
            time.sleep(1)

    def list_indexes(self) -> list:
        """
        Gets the list of vector data base indixes .
        """
        index_list = self._pincone_vdb_client.list_indexes().names()

        return index_list
    
    def delete_index(self,index_name:str) -> bool:
        """
        Delete a index .
        """
        try:
            self._pincone_vdb_client.delete_index(index_name)
            return True
        except Exception as e:
            logging.info(e)
            return False
    
    
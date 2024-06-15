from langchain_openai import AzureOpenAIEmbeddings
import os
import logging


AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_EMBEDDING_DEPLOYMENT = os.getenv("AZURE_EMBEDDING_DEPLOYMENT")
API_VERSION = os.getenv("API_VERSION")

class OpenAIEmbedding():
    def __init__(self)-> None:
        self.model = AZURE_EMBEDDING_DEPLOYMENT
     
        try:
            embeddings_model_client = AzureOpenAIEmbeddings(
                                                azure_deployment=self.model,
                                                openai_api_version=API_VERSION,
                                            )
            self.embeddings_model_client = embeddings_model_client
        except Exception as e:
            logging.error(f"Error connecting to azure endpoint: {e}")

    def get_embedding(self, text: str):      
        query_vector = self.embeddings_model_client.embed_query(text)  
        return query_vector
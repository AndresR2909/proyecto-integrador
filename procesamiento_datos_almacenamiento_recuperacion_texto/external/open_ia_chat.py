from langchain_openai import AzureChatOpenAI
from langchain_core.messages import HumanMessage
import os
import logging

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_DEPLOYMENT = os.getenv("AZURE_DEPLOYMENT")
API_VERSION = os.getenv("API_VERSION")
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")

class OpenAIChatModel():
    def __init__(self,temperature)-> None:
        self.temperature = temperature
        self.max_tokens = 1500
        self.model = AZURE_DEPLOYMENT
        self.request_timeout = 120
        self.max_retries = 6
        try:
            chat_model_client = AzureChatOpenAI( openai_api_version = API_VERSION ,
                                                azure_deployment = self.model,
                                                temperature = self.temperature,
                                                timeout = self.request_timeout,
                                                max_retries = self.max_retries,
                                                max_tokens = self.max_tokens)
            self.chat_model_client = chat_model_client
        except Exception as e:
            logging.error(f"Error connecting to azure endpoint: {e}")
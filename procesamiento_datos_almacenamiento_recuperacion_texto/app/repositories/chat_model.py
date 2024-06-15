from app.interfaces.chat_model import IChatmodel
from external.open_ia_embedding import OpenAIEmbedding
from external.open_ia_chat import OpenAIChatModel

import pandas as pd

class ChatModelRepository(IChatmodel):
    """Chat Model Repository"""

    def __init__(self, llm: OpenAIChatModel, embedding_model:OpenAIEmbedding):
        self._llm = llm
        self._embedding_model = embedding_model

    def chat(self, query)->str:  
        pass
    def chat_with_context(self, query, context)->str:  
        pass
    def chat_with_memory(self, query, memory)->str:  
        pass
    def chat_with_context_memory(self, context, memory)->str:  
        pass


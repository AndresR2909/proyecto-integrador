from abc import ABC, abstractmethod  
class ITextEmbedding(ABC):  
    @abstractmethod  
    def embed(self, text: str, filter)->list:  
        pass
    def embed_by_batch(self, text: list, filter)->list:  
        pass

def generar_embedding(texto:str, modelo:str=EMBEDDING_DEPLOYMENT)->list[float]:
    """Funcion para crear embedings a partir de un texto usando modelo model """
    return client.embeddings.create(input = [texto], model=modelo).data[0].embedding

def generar_embeddings_por_lote(textos:list, modelo:str=EMBEDDING_DEPLOYMENT)->list:
    """Funcion para crear embedings a partir de una lista de textos usando modelo model """
    return client.embeddings.create(input = textos, model=modelo).data
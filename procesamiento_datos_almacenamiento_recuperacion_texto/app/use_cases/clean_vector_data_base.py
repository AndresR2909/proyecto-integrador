from app.interfaces.document_loader import IVdbDocumentLoader
import logging

import pandas as pd


class CleanVectoreStore:  
    def __init__(self, loader: IVdbDocumentLoader):   
        self.loader = loader  
  
    def execute(self,index_name='youtube-transcription-index'):  
        
        #listar indices
        for i in self.loader.list_indexes_vdb():
            logging.info(self.loader.describe_index_vdb(i))
            #eliminar indices
            logging.info(self.loader.delete_index_vdb(i))
            #crear idice
    
        self.loader.create_index_vdb(index_name)

    
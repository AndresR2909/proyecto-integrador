from app.interfaces.document_loader import IVdbDocumentLoader

import pandas as pd


class LoadDataVectoreStore:  
    def __init__(self, loader: IVdbDocumentLoader):   
        self.loader = loader  
  
    def execute(self, df:pd.DataFrame,index_name='youtube-transcription-index',id_column="video_id",chunk_size=250,chunk_overlap=0):  
        
        crear = True
        for i in self.loader.list_indexes_vdb():
            if i==index_name:
                crear= False
                break
        
        if crear:
            #crear indice si no existe
            self.loader.create_index_vdb(index_name)

        #crear documentos 
        docs=self.loader.create_documents_from_df(df)
        
        #dividir documentos
        splitted_docs=self.loader.split_documents(docs,chunk_size,chunk_overlap)


        #agregar ids y metadata de particion a documentos
        preprocess_docs,ids = self.loader.create_ids_documents(splitted_docs,id_column=id_column)
       

        
        
        #cargar a vdb
        process_docs,id_list = self.loader.load_documents_vdb(index_name,preprocess_docs,ids)
        self.loader.describe_index_vdb(index_name)

        return process_docs
    
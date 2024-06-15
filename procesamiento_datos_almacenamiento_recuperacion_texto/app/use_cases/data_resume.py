from app.interfaces.agent_manager import IAgentManager
from app.interfaces.chat_model import IChatmodel
from app.interfaces.table_data_loader import ITableDataLoader
import pandas as pd 
from datetime import datetime
import logging
  
class SummaryDocuments:  
    def __init__(self, chat_model:IChatmodel , agent: IAgentManager, table_loader:ITableDataLoader):  
        self.chat_model = chat_model  
        self.agent = agent  
        self.table_loader = table_loader 
    def execute(self, documents: list, df: pd.DataFrame, table_name:str='SummaryTable')->dict:  
        timestamp = datetime.now()
        run_date = timestamp.strftime('%Y-%m-%d_%H')
        print('Agrupar documentos')
        dict_documents = self._group_documents(documents,df,column='video_id')
        tokens_by_docs =self.agent.count_tokens(documents)
        print(f"tokens transcripciones: {tokens_by_docs}")
        logging.info(f"tokens transcripciones: {tokens_by_docs}")
        print('Crear resumen')

        summary_by_video_list =[]
        for k,v in dict_documents.items():
            summary_by_channel_dict ={}
            tokens_by_docs =self.agent.count_tokens(v)
            print(f"tokens transcripciones {k}: {tokens_by_docs}")
            logging.info(f"tokens transcripciones {k}: {tokens_by_docs}")
            summary_by_video_dict ={}
            summary_by_video_dict["nombre_canal"] = v[0].metadata["chanel_name"]
            summary_by_video_dict["numero_documentos"] = len(v)
            summary_by_video_dict["numero_tokens_iniciales"] = self.agent.count_tokens(v)
            summary_by_video_dict["id_video"] = v[0].metadata["video_id"] 
            summary_by_video_dict["fecha_publicacion"] = v[0].metadata["publish_date"] 
            summary_by_video_dict["fuente"] = v[0].metadata["source"]
            summary_by_video_dict["titulo"] = v[0].metadata["title"]
            summary_by_video_dict["RowKey"] = run_date+"_"+k
            summary_by_video_dict["PartitionKey"] = k
            summary= self.agent.fitted_summary_documents(v)
            summary_by_video_dict["resumen"] = summary
            summary_by_video_list.append(summary_by_video_dict) 
        
        print(f"elementos a guardar {len(summary_by_video_list)}")
        print('guardar resumen')
        self.table_loader.load(summary_by_video_list,table_name)

        return summary_by_video_list
    
    def _group_documents(self,docs:list,df:pd.DataFrame,column='video_id', max_tokens =8000)->dict:
        # lista de canales
        values_list = list(df[column].unique())

        # Crear un diccionario para almacenar los documentos por canal
        docs_by_values = {value: [] for value in values_list}

        # Iterar sobre los documentos y agruparlos
        for doc in docs:
            value = doc.metadata[column]
            if value in docs_by_values:
                current_tokens = self.agent.count_tokens(docs_by_values[value])
                doc_tokens = self.agent.count_tokens(([doc]))
                # Check if adding the document would exceed the max token limit
                if current_tokens + doc_tokens > max_tokens:
                    # Find a new key for the next part of the group
                    part_number = 1
                    new_key = f"{value}_p{part_number}"
                    while new_key in docs_by_values:
                        part_number += 1
                        new_key = f"{value}_p_{part_number}"
                    docs_by_values[new_key] = [doc]
                else:
                    docs_by_values[value].append(doc)
        return docs_by_values
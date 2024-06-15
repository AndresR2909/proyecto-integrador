from app.interfaces.agent_manager import IAgentManager
from app.interfaces.chat_model import IChatmodel
from app.interfaces.table_data_loader import ITableDataLoader
import pandas as pd 
from datetime import datetime
import logging
  
class MapReduceSummaryDocuments:  
    def __init__(self, chat_model:IChatmodel , agent: IAgentManager, table_loader:ITableDataLoader):  
        self.chat_model = chat_model  
        self.agent = agent  
        self.table_loader = table_loader 
    def execute(self, documents: list, table_name:str='GeneralSummaryTable')->dict:  
        timestamp = datetime.now()
        run_date = timestamp.strftime('%Y-%m-%d')

        #Generar dataframe desde lista de diccionarios con resumenes
        df_sumary_by_video = pd.DataFrame(documents)

        # generar valores campos 
   
        print('Crear resumen')
        canales = ". ".join(str(x) for x in df_sumary_by_video['nombre_canal'].unique())
        id_videos = ". ".join(str(x) for x in df_sumary_by_video['id_video'].unique())
        fechas = ". ".join(str(x) for x in df_sumary_by_video['fecha_publicacion'].unique())
        titulos = ". ".join(str(x) for x in df_sumary_by_video['titulo'].unique())
        fuentes = ". ".join(str(x) for x in df_sumary_by_video['fuente'].unique())
        resumenes = "\n ".join(str(x) for x in df_sumary_by_video['resumen'].unique())
        id ="-".join(str(x) for x in df_sumary_by_video['id_video'].unique())

        tokens_by_docs =self.agent.count_tokens_from_string(resumenes)
        print(f"tokens transcripciones: {tokens_by_docs}")
        logging.info(f"tokens transcripciones: {tokens_by_docs}")


        summary_dict ={}
        summary_dict["nombre_canal"] = canales
        summary_dict["id_video"] = id_videos
        summary_dict["fecha_publicacion"] = fechas
        summary_dict["fuentes"] = fuentes
        summary_dict["titulos"] = titulos
        summary_dict["RowKey"] = id
        summary_dict["PartitionKey"] = run_date 
        summary= self.agent.map_reduce_custom_summary_documents(resumenes)
        summary_dict["resumen"] = summary
      
        print('guardar resumen general')
        self.table_loader.load([summary_dict],table_name)
        return summary_dict
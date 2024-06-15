from app.interfaces.data_loader import IDataLakeLoader
from app.interfaces.data_reader import IDataLakeReader


from datetime import datetime,timedelta,timezone 
import re
import pandas as pd


class DataPreprocessingDeltaCurated:  
    def __init__(self, reader: IDataLakeReader, loader: IDataLakeLoader):  
        self.reader = reader  
        self.loader = loader  
  
    def execute(self, source: str, destination: str,filename:str, delta_day:int,delta_hour:int, filter = None):  
        

        today= datetime.now() ##tz=timezone.utc
        print(f"hoy {today}")
        delta_day = timedelta(days=delta_day)
        delta_hour = timedelta(hours=delta_hour)
        delta_date = today-delta_day-delta_hour
        delta_date
        
        
        #leer archivos de raw
         
        df = self.reader.read_delta(source=source,
                                   path_name=filename,
                                   filter_date=delta_date,
                                   filter = filter) 
        # aplicar transformaciones
        transformed_df= self.preprocesar_textos(df)  
        
        #cargar a curated
        self.loader.write_parquet_one_file(df = transformed_df,
                                  container_name=destination,
                                  file_name=filename,
                                  partition_col='year' 
                                  )
        return transformed_df
    
    def _limpiar_emoticones(self,texto:str)->str:
        # Patrón para detectar emoticones
        patron_emoticones = r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002702-\U000027B0\U000024C2-\U0001F251\U0001f926-\U0001f937\U0001F9D1-\U0001F9DD]+'

        # Reemplazar los emoticones con un espacio en blanco
        texto_limpio = re.sub(patron_emoticones, '', texto)
        texto_limpio = texto_limpio.lower() 
        
        return texto_limpio

    def _limpiar_texto(self,texto:str):
        texto = texto.lower() 
        texto = re.sub(r'\s+',  ' ', texto).strip()
        texto = texto.strip()
        
        return texto


    def preprocesar_textos(self,df_clean:pd.DataFrame)->pd.DataFrame:

        df_clean['clean_title'] = df_clean['title'].apply(self._limpiar_emoticones)
        df_clean = df_clean.dropna(subset='caption_text_es', axis=0)
        df_clean["caption_text_es"] = df_clean["clean_title"] + ". " + df_clean["caption_text_es"].fillna(' ')
        df_clean['text']= df_clean["caption_text_es"].apply(self._limpiar_texto)
        df_clean =  df_clean.drop(columns=['caption_text_es','title'])

        select_columns=['chanel_name','video_id','url','publish_date','duration','last_update_date','clean_title','text','year']
        df_select = df_clean[select_columns]
        df_select = df_select.rename(columns={'url': 'source', 'clean_title': 'title'})
        
        return df_select

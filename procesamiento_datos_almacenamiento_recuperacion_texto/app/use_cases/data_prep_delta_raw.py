from app.interfaces.data_loader import IDataLakeLoader
from app.interfaces.data_reader import IDataReader,IDataLakeReader
from app.entities.transcripcion import TranscripRaw  
import pandas as pd 
from datetime import datetime,timedelta,timezone
import logging

  
class DataPreprocessingDeltaRaw:  
    def __init__(self, reader: IDataReader, loader: IDataLakeLoader, dl_reader:IDataLakeReader):  
        self.reader = reader  
        self.loader = loader 
        self.dl_reader = dl_reader 
  
    def execute(self, 
                source: str ,
                destination: str="raw", 
                filename: str='youtube_data',
                delta_day:int=0,
                delta_hour:int=0):  
        # fecha y hora actual utc
        today= datetime.now(tz=timezone.utc)
        delta_date = today-timedelta(days=delta_day)-timedelta(hours=delta_hour)
        logging.info(delta_date)
        df = self.reader.read_delta(delta_date)
        if len(df)>0:
            logging.info(df.info())
    
        # Aquí puedes hacer las transformaciones que necesites con las transcripciones
        df_clean = self.preprocesar_nulos_duplicados(df)
        df_clean = self.eliminar_columnas(df_clean)

        df_clean['year'] = df_clean['publish_date'].apply(lambda x: int(x[0:4]))
        df_clean['last_update_date'] = datetime.now().strftime('%Y-%m-%d')
        
        year_filename = f"{filename}/{filename}_{today.year}"
        df_temp = self.dl_reader.read_parquet(destination,year_filename,filter=None)

        df_join = pd.concat([df_temp, df_clean]).drop_duplicates(subset='video_id').reset_index(drop=True)

        self.loader.write_parquet_one_file(df_join, container_name=destination, file_name=filename, partition_col='year')

        return df_join

    def preprocesar_nulos_duplicados(self,df:pd.DataFrame)->pd.DataFrame:
        #eliminar duplicados
        df_clean = df.drop_duplicates(subset='video_id').reset_index(drop=True).copy()
        
        # Completar los datos nulos de 'duration' con los valores de 'total_length'
        df_clean['duration'] = df_clean['duration'].fillna(df_clean['total_length'])

        # eliminar registros con mas del 50% de columnas nulas
        df_clean = df_clean.dropna(thresh= len(df_clean)/2 , axis=1)

        return df_clean

    def eliminar_columnas(self,df:pd.DataFrame)->pd.DataFrame:
        #eliminar columnas no usadas
        drop_columns = ['Unnamed: 0','keywords','description','total_length','total_views','relativeDateText']

        df_colums= df.columns.to_list()

        drop_columns = [elemento for elemento in drop_columns if elemento in df_colums]

        df_clean =  df.drop(columns=drop_columns)
        return df_clean



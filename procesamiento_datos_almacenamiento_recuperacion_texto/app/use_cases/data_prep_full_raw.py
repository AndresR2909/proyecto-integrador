from app.interfaces.data_loader import IDataLakeLoader
from app.interfaces.data_reader import IDataReader
from app.entities.transcripcion import TranscripRaw  
import pandas as pd 
from datetime import datetime,timedelta,timezone
import logging
from io import StringIO
  
class DataPreprocessingFullRaw:  
    def __init__(self, reader: IDataReader, loader: IDataLakeLoader):  
        self.reader = reader  
        self.loader = loader 
  
    def execute(self, 
                source: str ,
                destination: str="raw", 
                filename: str='youtube_data'):  
        # fecha y hora actual utc
        df = self.reader.read_full()
        if len(df)>0:
            df_info = StringIO("")
            df.info(df_info)
            logging.info(df.info())
    
        # Aquí puedes hacer las transformaciones que necesites con las transcripciones
        df_clean = self.preprocesar_nulos_duplicados(df)
        df_clean = self.eliminar_columnas(df_clean)

        df_clean['year'] = df_clean['publish_date'].apply(lambda x: int(x[0:4]))
        df_clean['last_update_date'] = datetime.now().strftime('%Y-%m-%d')

        self.loader.write_parquet_one_file(df_clean, container_name=destination, file_name=filename, partition_col='year')

        return df_clean

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



from tqdm import tqdm
from datetime import datetime
import pandas as pd
from logic.data_from_youtube import YouTubeScraper
yt_sc = YouTubeScraper()

class DataIngestion:
    def __init__(self,url_channel_list:list):
        self.youtube_sc = yt_sc
        self.url_channel_list = url_channel_list
    def __init__(self):
        self.youtube_sc = yt_sc

    def full_load_from_youtube_channel(self,url_channel_list:list,output_path:str,filename:str)->list:
        """ Carga completa de informacion de lista de canales 
        """
        date =datetime.now().strftime('%Y-%m-%d')
        contador = 0
        df_list = []
        for channel_url in tqdm(url_channel_list, desc='Procesando urls'):
            df_metadata = yt_sc.create_full_dataset_from_channel_url(channel_url)
            print(len(df_metadata))
            df_metadata.to_csv(f'{output_path}/{filename}_{date}_{contador}.csv', sep = ';')
            contador += 1
            df_list.append(df_metadata)

        return df_list
    
    def delta_load_from_youtube_channel_by_days(self,url_channel_list:list, delta_days:int)->pd.DataFrame:
        """ Carga delta de informacion de lista de canales 
        """
        date =datetime.now().strftime('%Y-%m-%d')
        contador = 0
        df_list = []
        for channel_url in tqdm(url_channel_list, desc='Procesando urls'):
            df_metadata = yt_sc.create_delta_dataset_from_channel_url(channel_url, delta_days)
            print(len(df_metadata))
            if len(df_metadata) != 0:
                contador += 1
                df_list.append(df_metadata)
        if len(df_list) != 0:
            df_concat = pd.concat(df_list, ignore_index=True)    
        else:
            df_concat = pd.DataFrame([])    

        return df_concat
    
    def delta_load_from_youtube_channel_by_hours(self,url_channel_list:list, delta_hours:int)->pd.DataFrame:
        """ Carga delta de informacion de lista de canales 
        """
        date =datetime.now().strftime('%Y-%m-%d')
        contador = 0
        df_list = []
        for channel_url in tqdm(url_channel_list, desc='Procesando urls'):
            df_metadata = yt_sc.create_delta_hours_dataset_from_channel_url(channel_url, delta= delta_hours)
            if len(df_metadata) != 0:
                contador += 1
                df_list.append(df_metadata)
        if len(df_list) != 0:
            df_concat = pd.concat(df_list, ignore_index=True)    
        else:
            df_concat = pd.DataFrame([])    

        return df_concat
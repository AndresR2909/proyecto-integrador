from youtube_transcript_api import YouTubeTranscriptApi
from pytube import YouTube 
import yt_dlp
import pandas as pd
from datetime import datetime, timedelta
import yt_dlp
import pandas as pd
import logging

class YouTubeScraper:
    def __init__(self):
        # Configuración de yt-dlp para obtener la metadata y título de los videos del canal
        self.ydl_opts = {
            'quiet': True,  # Para desactivar la salida en la consola
            'ignoreerrors': True,
            'extract_flat': True,  # Extrae la información en formato plano para un archivo CSV
            'skip_download': True  # No descargar los videos, solo obtener la información
        }

    def _get_metadata_from_youtube_channel_url(self, url:str)->dict:
        """Funion para obtener metadata de url de canal de yotutube"""
        # Crea el objeto yt-dlp y descarga la información del canal
        with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
            # Obtiene la información del canal
            info_dict = ydl.extract_info(url, download=False)
        return info_dict
    
    def _get_youtube_video_info_from_url(self,link:str)->dict:
        """Agregar metadatos al video youtube"""
        try:
            yt = YouTube(link)
            try: 
                relativeDateText = yt.initial_data['contents']['twoColumnWatchNextResults']['results']['results']['contents'][0]['videoPrimaryInfoRenderer']['relativeDateText']['simpleText']
            except:
                relativeDateText = ""
            metadata_dict = {
                'channel_id': yt.channel_id,
                'video_id': yt.video_id,
                'title': yt.title,
                'author': yt.author,
                'keywords': yt.keywords,
                'description': yt.description,
                'publish_date': yt.publish_date,
                'total_length': yt.length,
                'total_views': yt.views,
                'video_rating': yt.rating if yt.rating else None,
                'relativeDateText': relativeDateText
            }
        except Exception as e:
            metadata_dict = None
            print(f"Something went wrong with youtube link {e}")
        

        return metadata_dict 

    def _download_transcripts(self,video_id:str)->dict:
        """Agregar las transcripciones a cada video_id"""
        # Variables forstore the downloaded captions:
        transcripts_dict = {}
        caption = None
        captions_text = ''

        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        except Exception as e:
            transcript_list = None
            print(f"Something went wrong with transcript_list {e}")
        
        if transcript_list: 
            # Loop all languages available for this video and download the generated captions:
            for x, tr in enumerate(transcript_list):
                
                try:
                    #print("Downloading captions in " + tr.language + "...")
                    transcript_obtained_in_language = transcript_list.find_transcript([tr.language_code]).fetch()
                    for segment in transcript_obtained_in_language:
                        caption = segment['text']
                        captions_text += caption + ' '

                except Exception as e:
                    print(f"Something went wrong transcript obtained in language: {e}")
                else:
                    pass
                    #print("Done")
                    
                key_value = f'caption_text_{tr.language_code}'
                transcripts_dict[key_value] = captions_text
                caption = None
                captions_text = ''    
            
        return transcripts_dict

    def _filter_delta_hours(self,relativeDateText:str,publish_date:datetime|str,delta_hours:int=24)->bool:
        """Calcular fecha de dias delta para filtrar la extraccion de 
        la informacion
        """
        #fecha actual
        #today = datetime.now()
        today_utc = datetime.now()
                          
        # Convertir la fecha de publicación a datetime
        if type(publish_date) == str:
            publish_date_dt = datetime.strptime(publish_date, '%Y-%m-%d %H:%M:%S')
        else:
            publish_date_dt = publish_date

        publish_date_dt = self._clean_relative_date(relativeDateText, publish_date_dt)
  
        # Restar los días a la fecha actual
        delta_date = today_utc - timedelta(hours=delta_hours)
        
        print(f'delta_date: {delta_date}')
        print(f'published_date: {publish_date_dt}')

        # Comparar las fechas
        if publish_date_dt >= delta_date:
            return True
        else:
            return False

    def _filter_delta_days(self,publish_date:datetime|str,delta_days:int=1):
        """Calcular fecha de dias delta para filtrar la extraccion de 
        la informacion
        """
        #fecha actual
        #today = datetime.now()
        today_utc = datetime.now(datetime.timezone.utc)
                          
        # Convertir la fecha de publicación a datetime
        if type(publish_date) == str:
            publish_date_dt = datetime.strptime(publish_date, '%Y-%m-%d %H:%M:%S')
        else:
            publish_date_dt = publish_date
        print(publish_date_dt)
        # Restar los días a la fecha actual
        delta_date = today_utc - timedelta(days=delta_days)
        # Comparar las fechas
        if publish_date_dt >= delta_date:
            return True
        else:
            return False

    def _filter_delta_days_v2(self,relativeDateText:str,publish_date:datetime|str,delta_days:int=1):
        """Calcular fecha de dias delta para filtrar la extraccion de 
        la informacion
        """
        #fecha actual
        #today = datetime.now()
        today_utc = datetime.now()
                          
        # Convertir la fecha de publicación a datetime
        if type(publish_date) == str:
            publish_date_dt = datetime.strptime(publish_date, '%Y-%m-%d %H:%M:%S')
        else:
            publish_date_dt = publish_date

        publish_date_dt = self._clean_relative_date(relativeDateText, publish_date_dt)
  
        # Restar los días a la fecha actual
        delta_date = today_utc - timedelta(days=delta_days)
        
        print(f'delta_date: {delta_date}')
        print(f'published_date: {publish_date_dt}')

        # Comparar las fechas
        if publish_date_dt >= delta_date:
            return True
        else:
            return False

    def _clean_relative_date(self,relativeDateText: str, publish_date: datetime | str):
        # Eliminar "Streamed" si está presente
        relativeDateText = relativeDateText.replace('Streamed ', '')

        # Obtener la fecha actual en formato UTC
        today = datetime.now()
        try:
            # Convertir el texto a timedelta
            if 'minute' in relativeDateText:
                minutes = int(relativeDateText.split()[0])
                calculated_date = today - timedelta(minutes=minutes)
            elif 'hour' in relativeDateText:
                hours = int(relativeDateText.split()[0])
                calculated_date = today - timedelta(hours=hours)
            elif 'day' in relativeDateText:
                days = int(relativeDateText.split()[0])
                calculated_date = today - timedelta(days=days)
            elif 'month' in relativeDateText:
                months = int(relativeDateText.split()[0])
                calculated_date = today - timedelta(days=months * 30)  # Assuming 30 days per month for simplicity
            elif 'year' in relativeDateText:
                years = int(relativeDateText.split()[0])
                calculated_date = today - timedelta(days=years * 365)  # Assuming 365 days per year for simplicity
            else:
                calculated_date = publish_date
        except:
            calculated_date = publish_date

        return calculated_date

    def create_full_dataset_from_channel_url(self,channel_url:str)->pd.DataFrame:
        """Crear dataframe con la informacion extraida de cada video
        del canal de la url ingresada como parametro.
        """
        video_metadata_list =[]
        info_dict = self._get_metadata_from_youtube_channel_url(channel_url)
        if info_dict:
            chanel_name = info_dict['channel']
            chanel_id = info_dict['channel_id']
            chanel_url = info_dict['channel_url']
            print(f"extrayendo informacion de canal {chanel_name}")
            for video in info_dict['entries']: 
                
                video_url = video['url']
                video_id =  video['id']

                metadata_dict = self._get_youtube_video_info_from_url(video_url)
                transcripts = self._download_transcripts(video_id)
                
                video_metadata = {
                                    'chanel_name' : chanel_name,
                                    'chanel_id' : chanel_id,
                                    'chanel_url' : chanel_url,
                                    'video_id': video_id,
                                    'title': video['title'], 
                                    'url': video_url , 
                                    'keywords': metadata_dict['keywords'],
                                    'publish_date': metadata_dict['publish_date'],
                                    'relativeDateText': metadata_dict['relativeDateText'],
                                    'total_length': metadata_dict['total_length'],
                                    'total_views': metadata_dict['total_views'],
                                    'video_rating': metadata_dict['video_rating'],
                                    'description': video['description']
                                }
                video_metadata = {**video_metadata,**transcripts}
                video_metadata_list.append(video_metadata)
            video_metadata_df = pd.DataFrame(video_metadata_list)
        else:
            logging.info(f'info_dict: {info_dict}, error al extraer datos de url {channel_url}')
            video_metadata_df = pd.DataFrame([])
        return video_metadata_df
    
    def create_delta_dataset_from_channel_url(self,channel_url:str, delta_days:int=0)->pd.DataFrame:
        """Crear dataframe con la informacion extraida de cada video, publicado dentro de los delta_days
        , de la url del canal ingresado como parametro.
        """
        video_metadata_list =[]
        info_dict = self._get_metadata_from_youtube_channel_url(channel_url)
        if info_dict:
            chanel_name = info_dict['channel']
            chanel_id = info_dict['channel_id']
            chanel_url = info_dict['channel_url']
            print(f"extrayendo informacion de canal {chanel_name}")
            for video in info_dict['entries']:  
                video_url = video['url']
                video_id =  video['id']
                metadata_dict = self._get_youtube_video_info_from_url(video_url)
                publish_date = metadata_dict['publish_date']
                relativeDateText = metadata_dict['relativeDateText']
                #condicion_delta = self._filter_delta_days(publish_date,delta_days)
                condicion_delta = self._filter_delta_days_v2(relativeDateText,publish_date,delta_days)
                print(condicion_delta)
                if condicion_delta:
                    transcripts = self._download_transcripts(video_id)
                    
                    video_metadata = {
                                        'chanel_name' : chanel_name,
                                        'chanel_id' : chanel_id,
                                        'chanel_url' : chanel_url,
                                        'video_id': video_id,
                                        'title': video['title'], 
                                        'url': video_url , 
                                        'keywords': metadata_dict['keywords'],
                                        'publish_date': metadata_dict['publish_date'],
                                        'relativeDateText': metadata_dict['relativeDateText'],
                                        'total_length': metadata_dict['total_length'],
                                        'total_views': metadata_dict['total_views'],
                                        'video_rating': metadata_dict['video_rating'],
                                        'description': video['description'], 
                                        'duration': int(video['duration']) if video['duration'] else None
                                    }
                    video_metadata = {**video_metadata,**transcripts}
                    video_metadata_list.append(video_metadata)
                else:
                    video_metadata_df = pd.DataFrame(video_metadata_list)
                    break
        else:
            logging.info(f'info_dict: {info_dict}, error al extraer datos de url {channel_url}')
            video_metadata_df = pd.DataFrame([])

        return video_metadata_df
    
    def create_delta_hours_dataset_from_channel_url(self,channel_url:str, delta:int=24)->pd.DataFrame:
        """Crear dataframe con la informacion extraida de cada video, publicado dentro de los delta_days
        , de la url del canal ingresado como parametro.
        """
        video_metadata_list =[]
        info_dict = self._get_metadata_from_youtube_channel_url(channel_url)
        if info_dict:
            chanel_name = info_dict['channel']
            chanel_id = info_dict['channel_id']
            chanel_url = info_dict['channel_url']
            print(f"extrayendo informacion de canal {chanel_name}")
            for video in info_dict['entries']:  
                video_url = video['url']
                video_id =  video['id']
                metadata_dict = self._get_youtube_video_info_from_url(video_url)
                publish_date = metadata_dict['publish_date']
                relativeDateText = metadata_dict['relativeDateText']
                condicion_delta = self._filter_delta_hours(relativeDateText,publish_date,delta)
                if condicion_delta:
                    transcripts = self._download_transcripts(video_id)
                    
                    video_metadata = {
                                        'chanel_name' : chanel_name,
                                        'chanel_id' : chanel_id,
                                        'chanel_url' : chanel_url,
                                        'video_id': video_id,
                                        'title': video['title'], 
                                        'url': video_url , 
                                        'keywords': metadata_dict['keywords'],
                                        'publish_date': metadata_dict['publish_date'],
                                        'relativeDateText': metadata_dict['relativeDateText'],
                                        'total_length': metadata_dict['total_length'],
                                        'total_views': metadata_dict['total_views'],
                                        'video_rating': metadata_dict['video_rating'],
                                        'description': video['description'], 
                                        'duration': int(video['duration']) if video['duration'] else None
                                    }
                    video_metadata = {**video_metadata,**transcripts}
                    video_metadata_list.append(video_metadata)
                else:
                    video_metadata_df = pd.DataFrame(video_metadata_list)
                    break
        else:
            logging.info(f'info_dict : {info_dict}, no se extrajo informacion de url: {channel_url}')
            video_metadata_df = pd.DataFrame([])
        return video_metadata_df
    
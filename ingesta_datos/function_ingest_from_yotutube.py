import azure.functions as func
import logging
from datetime import datetime
from logic.data_ingestion import DataIngestion
from logic.blob_storage import BlobStorageManager
import os
import io
from dotenv import load_dotenv
load_dotenv()

DELTA_DAYS = os.environ.get('DELTA_DAYS')
DELTA_HOURS = os.environ.get('DELTA_HOURS')
CHANNEL_LIST = os.environ.get('CHANNEL_LIST')
SCHEDULE = os.environ.get('CRON_SCHEDULE')

bp1 = func.Blueprint() 
data_ingestion = DataIngestion()
bsm = BlobStorageManager()

@bp1.function_name(name="youtube_ingest_timer_trigger")
@bp1.timer_trigger(schedule=SCHEDULE,
              arg_name="mytimer",
              run_on_startup=False) 
@bp1.blob_output(arg_name="outputblob",
                path="landing/youtube_delta_data.csv",
                connection="CONNECTION_STRING")
def timer_trigger_function(mytimer: func.TimerRequest, outputblob: func.Out[str]) -> None:
    timestamp = datetime.now()
    if CHANNEL_LIST:
        # Convertir la cadena de elementos separados por comas en una lista
        chanel_list = CHANNEL_LIST.split(',')
        logging.info(chanel_list)
        if DELTA_DAYS and (DELTA_DAYS != "0"):
            delta_days = int(DELTA_DAYS)
            logging.info(f'delta days: {delta_days}') 
            df_delta = data_ingestion.delta_load_from_youtube_channel_by_days(url_channel_list=chanel_list, 
                                                    delta_days=delta_days )
            if len(df_delta) != 0:
                data = df_delta.to_csv(header=True,index=False,sep=";",mode='w')
                logging.info(f'Numero de registros: {len(df_delta)}')
            else:
                logging.info(f'df_delta sin registros')
                data = None

        elif DELTA_HOURS: 
            delta_hours = int(DELTA_HOURS)
            logging.info(f'delta hours: {delta_hours}')
            df_delta = data_ingestion.delta_load_from_youtube_channel_by_hours(url_channel_list=chanel_list, 
                                                        delta_hours=delta_hours )
            if len(df_delta) != 0:
                data = df_delta.to_csv(header=True,index=False,sep=";",mode='w')
                logging.info(f'Numero de registros: {len(df_delta)}')
            else:
                logging.info(f'df_delta sin registros')
                data = None
        else:   
            data = None
            logging.info(f'lack delta parameters')
            
        if data:    
            buffer = io.StringIO()
            df_delta.info(buf=buffer)
            s = buffer.getvalue()
            logging.info(s)
            #outputblob.set(data)
            file_path = f"youtube_delta_data_{timestamp.strftime('%Y-%m-%d-%H_%M')}.csv"
            bsm.upload_blob(file_path,data)
        else:
            logging.info('No se ingestaron datos')
    else:
        logging.info('please, configure the channel list urls')

    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', timestamp.isoformat())

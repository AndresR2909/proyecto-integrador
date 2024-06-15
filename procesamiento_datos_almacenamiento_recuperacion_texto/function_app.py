import azure.functions as func
import azure.durable_functions as df
import os
import logging
from datetime import datetime
import pandas as pd
from io import StringIO

from external.blob_storage import BlobStorage
from external.data_lake_pandas import DataLakePandas
from external.data_lake_storage import DataLakeStorage
from external.lang_chain_manager import LangChainManager, Document
from external.pinecone_vdb import PineconeVectorDB
from external.open_ia_chat import OpenAIChatModel
from external.azure_table import AzureTableStorage

from app.repositories.pandas_loader import DataLakeLoader
from app.repositories.blob_storage_reader import BlobStorageReader
from app.repositories.pandas_reader import DataLakeReader
from app.repositories.pinecone_vdb_loader import PinceconeVDBRepository
from app.repositories.chat_model import ChatModelRepository
from app.repositories.agent_manager import AgentManagerRepository
from app.repositories.table_data_loader import TableDataLoader

from app.use_cases.data_prep_delta_curated import DataPreprocessingDeltaCurated
from app.use_cases.data_prep_delta_raw import DataPreprocessingDeltaRaw 
from app.use_cases.clean_vector_data_base import CleanVectoreStore
from app.use_cases.data_to_vector_data_base import LoadDataVectoreStore
from app.use_cases.data_resume import SummaryDocuments
from app.use_cases.data_map_reduce_resume import MapReduceSummaryDocuments


from dotenv import load_dotenv
load_dotenv()


SCHEDULE = os.environ.get('CRON_SCHEDULE')
DELTA_DAYS = os.environ.get('DELTA_DAYS')
DELTA_HOURS = os.environ.get('DELTA_HOURS')



container_name = "landing"
blob_storage = BlobStorage(container_name)
bs_reader = BlobStorageReader(blob_storage)
dl_pandas = DataLakePandas()
dl_loader = DataLakeLoader(dl_pandas)
langchain_manager = LangChainManager()
pinecone_vdb = PineconeVectorDB()
open_ia_model = OpenAIChatModel(temperature=0.1)
azure_table = AzureTableStorage()

table_data_loader = TableDataLoader(azure_table)
dl_storage = DataLakeStorage("raw")
dl_reader = DataLakeReader(dl_pandas,dl_storage)
vdb_repository= PinceconeVDBRepository(pinecone_vdb,langchain_manager)
chat_repository = ChatModelRepository(open_ia_model,langchain_manager)
agent_repository =AgentManagerRepository(open_ia_model,langchain_manager)

use_case_delta_raw = DataPreprocessingDeltaRaw(bs_reader, dl_loader, dl_reader)  
use_case_delta_curated = DataPreprocessingDeltaCurated(dl_reader, dl_loader)  
load_vdb_use_case = LoadDataVectoreStore(vdb_repository)
summary_use_case = SummaryDocuments(chat_repository,agent_repository, table_data_loader)
use_case_map_reduce_summary = MapReduceSummaryDocuments(chat_repository,agent_repository,table_data_loader)

#bp = df.Blueprint(http_auth_level=func.AuthLevel.ANONYMOUS)
myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@myApp.function_name(name="timer_trigger")
@myApp.timer_trigger(schedule=SCHEDULE,
              arg_name="mytimer",
              run_on_startup=False) 
@myApp.durable_client_input(client_name="client")
async def timer_trigger_function(mytimer: func.TimerRequest,client:df.DurableOrchestrationClient) -> None:
    timestamp = datetime.now()
    logging.info('Python timer trigger function running at %s', timestamp.isoformat())
    await client.start_new('orchestrator')
    if mytimer.past_due:
        logging.info('The timer is past due!')
    timestamp = datetime.now()
    logging.info('trigger function ran at %s', timestamp.isoformat())


# Orchestrator
@myApp.orchestration_trigger(context_name="context")
def orchestrator(context:df.DurableOrchestrationContext):
    logging.info("orquestador")
    filename= "data_from_youtube"
    df_ingest = yield context.call_activity("ingest_data", filename)
    df_process = yield context.call_activity("preprocess_data", df_ingest)
    data = yield context.call_activity("store_vector_db", df_process)
    summaries = yield context.call_activity("create_resume", data)
    final = yield context.call_activity("create_map_reduce_resume", summaries)

    return final

# ingest Activity
@myApp.activity_trigger(input_name="filename")
def ingest_data(filename: str):
    logging.info("Ingest data: external to raw")
    container_source = "landing"
    container_sink = "raw"
    filter = None
    delta_day= int(DELTA_DAYS)
    delta_hour= int(DELTA_HOURS)
    df_raw = use_case_delta_raw.execute(container_source, 
                            container_sink,
                            filename=filename,
                            delta_day=delta_day,
                            delta_hour= delta_hour
                            )

    return df_raw.to_json()

# etl delta Activity
@myApp.activity_trigger(input_name="dataframe")
def preprocess_data(dataframe: str):
    logging.info(f"preprocess data: raw to curated")
    json_buffer = StringIO(dataframe)
    df = pd.read_json(json_buffer)
    delta_day= int(DELTA_DAYS)
    delta_hour= int(DELTA_HOURS)
    container_source = "raw"
    container_sink = "curated"
    filename= "data_from_youtube"
    filter = None
    df_curated = use_case_delta_curated.execute(container_source, 
                                                container_sink,
                                                filename,
                                                delta_day,
                                                delta_hour, 
                                                filter)
    
    last_df = df_curated[df_curated['last_update_date']==max(df_curated['last_update_date'])]
    return last_df.to_json()


# Activity
@myApp.activity_trigger(input_name="dataframe")
def store_vector_db(dataframe: str):
    json_buffer = StringIO(dataframe)
    df = pd.read_json(json_buffer)
    logging.info(f"store_vector_db")
    process_docs = load_vdb_use_case.execute(df,chunk_size=250, chunk_overlap=0)
    data = str({0:dataframe, 1:str(process_docs)})
    return data

# Activity
@myApp.activity_trigger(input_name="documents")
def create_resume(documents: str):
    
    data_in_list=eval(documents)
    logging.info("convertir a df")
    json_buffer = StringIO(data_in_list[0])
    df_info = StringIO("")
    df = pd.read_json(json_buffer)
    df.info(df_info)
    logging.info(df_info)
    logging.info("convertir a lista documentos")
    docs = eval(data_in_list[1])
    result = summary_use_case.execute(docs,df,"VideoSummaryTable")
    string_result = str({0:str(result)})
    return string_result

# Activity
@myApp.activity_trigger(input_name="documents")
def create_map_reduce_resume(documents: str):
    data_in_list=eval(documents)
    logging.info("convertir lista")
    docs = eval(data_in_list[0])
    use_case_map_reduce_summary.execute( docs, table_name='GeneralSummaryTable')
    return True


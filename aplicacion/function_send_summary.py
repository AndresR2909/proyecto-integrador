import azure.functions as func
import logging
from datetime import datetime
import os

from external.azure_table import AzureTableStorage
from external.twilio import TwilioMessaging
from app.repositories.message_sender import MessageSender
from app.repositories.table_data_reader import TableDataReader
from app.use_cases.data_from_table_to_wapp import SendMessageWhatsapp

from dotenv import load_dotenv
load_dotenv()

SCHEDULE = os.environ.get('CRON_SCHEDULE')

bp1 = func.Blueprint()

azure_table = AzureTableStorage()
twilio = TwilioMessaging()

table_reader = TableDataReader(azure_table)
msg_sender = MessageSender(twilio)

use_case_send_summary =  SendMessageWhatsapp(table_reader,msg_sender)

@bp1.function_name(name="send_summary")
@bp1.timer_trigger(schedule=SCHEDULE,
              arg_name="mytimer",
              run_on_startup=False) 
def timer_trigger_send_summary_function(mytimer: func.TimerRequest) -> None:
    timestamp = datetime.now()

    try:
        summaries = use_case_send_summary.execute('GeneralSummaryTable')
        logging.info(f'enviados {len(summaries)} resumenes')
    except Exception as e:
        msn = f'Error {e} , enviando msg a whatsapp'
        logging.info(msn)
        print(msn)

    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', timestamp.isoformat())

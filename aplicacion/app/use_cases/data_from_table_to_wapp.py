
from app.interfaces.message_sender import IMessageSender
from app.interfaces.table_data_loader import ITableDataReader
import time
from datetime import datetime


class SendMessageWhatsapp:  
    def __init__(self, table_query: ITableDataReader, msg_sender: IMessageSender):  
        self.table_query = table_query  
        self.msg_sender = msg_sender  
        self.fecha_formateada = None
  
    def execute(self,table_name:str='GeneralSummaryTable', to='+573153800409'):  
    
        # Obtener la fecha actual
        fecha_actual = datetime.now()
        self.fecha_formateada = fecha_actual.strftime("%Y-%m-%d")
        print(f"hoy {self.fecha_formateada}")

        #leer entidades tabla con resumen
        my_filter = f"PartitionKey eq '{self.fecha_formateada}'"
        entities = self.table_query.query_table(query=my_filter,table_name=table_name)
        list_summaries = [sumaries['resumen'] for sumaries in entities]
        
        # enviar mensaje  
        body=f'Resumen videos proyecto integrador {self.fecha_formateada}: '
        self.msg_sender.send_message(msg = body,receiver=to)

        
        for summary in list_summaries:
            for m in summary.split(sep = '\n\n'):
                body=f'{m}'
                self.msg_sender.send_message(msg = body,receiver=to)
                time.sleep(4)

        return list_summaries

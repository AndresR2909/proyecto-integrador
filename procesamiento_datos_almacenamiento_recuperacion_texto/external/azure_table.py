from azure.data.tables import TableServiceClient, UpdateMode
from azure.data.tables import EntityProperty, EdmType
import os
import logging
from dotenv import load_dotenv  
  
# Cargar las variables de entorno desde el archivo .env  
load_dotenv()  

connection_string = os.environ.get('CONNECTION_STRING')



class AzureTableStorage():
    def __init__(self):
        try: 
            #conect azure table
            self.table_service = TableServiceClient.from_connection_string(conn_str=connection_string)
        except Exception as e:
            logging.info(f"error connecting azure table :  {e}")
            print(f"error connecting azure table :  {e}")

    def load_entity(self, entity, table_name:str):
        try: 
            table_client = self.table_service.get_table_client(table_name=table_name)
            #load entity into table
            table_client.upsert_entity(entity, mode = UpdateMode.MERGE)
        except Exception as e:
            logging.info(f"error loading entity into azure table {table_name}:  {e}")
        
    def load_all_entities(self, entities:list, table_name:str):
        inserted_rows=0
        error_rows=0
        for entity in entities:
            try:
                table_client = self.table_service.get_table_client(table_name=table_name)
                #load entity into table
                table_client.upsert_entity(entity , mode = UpdateMode.MERGE)
                inserted_rows += 1
            except Exception as e:
                error_rows += 1
                logging.info(f'Error: {e} , type: {type(e)}')
                print(f'Error: {e} , type: {type(e)}')
            
        logging.info(f'{inserted_rows} rows added/updated into azure table "{table_name}"')
        logging.info(f'{error_rows} rows not added/updated into azure table "{table_name}"')

    def cast_number_values(self,value):
        if isinstance(value, int):
            return EntityProperty(value=value, edm_type=EdmType.INT64)
        elif isinstance(value, float):
            return EntityProperty(value=value, edm_type=EdmType.DOUBLE)
        else:
            return value
    
      

    
from app.interfaces.table_data_loader import ITableDataReader
from external.azure_table import AzureTableStorage


class TableDataReader(ITableDataReader):
    def __init__(self,table_storage: AzureTableStorage):
        self.table_storage = table_storage 
        pass

    def query_table(self,query:str, table_name:str):
        query_entities = self.table_storage.query_entity(filter=query,table_name=table_name)
        return query_entities


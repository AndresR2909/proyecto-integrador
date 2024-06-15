import pandas as pd
from app.interfaces.table_data_loader import ITableDataLoader
from external.azure_table import AzureTableStorage


class TableDataLoader(ITableDataLoader):
    def __init__(self,table_storage: AzureTableStorage):
        self.table_storage = table_storage 
        pass

    def load(self,entities_list:list, table_name:str):
        self.table_storage.load_all_entities(entities_list, table_name)


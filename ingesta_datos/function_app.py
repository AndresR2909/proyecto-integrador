import azure.functions as func 
from function_ingest_from_yotutube import bp1


app = func.FunctionApp() 

app.register_functions(bp1) 

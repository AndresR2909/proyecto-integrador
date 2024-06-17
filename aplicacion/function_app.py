import azure.functions as func 
from function_send_summary import bp1


app = func.FunctionApp() 

app.register_functions(bp1) 


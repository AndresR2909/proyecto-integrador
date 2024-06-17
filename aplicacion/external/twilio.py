import os
from twilio.rest import Client
import logging
from dotenv import load_dotenv  
  
# Cargar las variables de entorno desde el archivo .env  
load_dotenv()  

account_sid = os.environ.get('account_sid')
auth_token = os.environ.get('auth_token')


class TwilioMessaging():
    def __init__(self):
        try: 
            #conect azure table
            self.client = Client(account_sid, auth_token)
        except Exception as e:
            logging.info(f"error connecting Twilio :  {e}")
            print(f"error connecting Twilio :  {e}")

    def send_msg_whatsapp(self, body:str, number:str='+573153800409'):
        try: 
          message = self.client.messages.create(
                    from_='whatsapp:+14155238886',
                    body=body,
                    to=f'whatsapp:{number}'
          )
        except Exception as e:
            logging.info(f"error sending message :  {e}")
        
    
      

    
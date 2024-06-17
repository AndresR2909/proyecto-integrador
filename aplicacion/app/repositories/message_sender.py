from app.interfaces.message_sender import IMessageSender
from external.twilio import TwilioMessaging


class MessageSender(IMessageSender):
    def __init__(self,message_sender: TwilioMessaging):
        self.message_sender = message_sender 
        pass

    def send_message(self,msg:str, receiver:str):
        message = self.message_sender.send_msg_whatsapp(body=msg,number=receiver)
        return message


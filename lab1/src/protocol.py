"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
import time
import sys
from socket import socket


class Message:
    """Message Type."""
    def __init__(self,command=None):
        self.command = command

    
class JoinMessage(Message):
    """Message to join a chat channel."""
    def __init__(self,channel=None,command="join"):
        super().__init__(command)
        self.channel = channel
        
    def __str__(self):
        dicionario = {
            "command" : self.command,
            "channel" : self.channel
        }
        j=json.dumps(dicionario)
        return j

    def __retChannel__(self):
        return self.channel


class RegisterMessage(Message):
    """Message to register username in the server."""
    def __init__(self,user,command="register"):
        super().__init__(command)
        self.user = user
        
    def __str__(self):
        dicionario = {
            "command" : self.command,
            "user" : self.user
        }
        j=json.dumps(dicionario)
        return j

    
class TextMessage(Message):
    """Message to chat with other clients."""
    def __init__(self,message,channel,command="message"):
        super().__init__(command)
        self.message = message
        self.channel = channel
        self.ts = int(time.time())
    def __str__(self):
        if self.channel==None:
            dicionario = {
                "command" : self.command,
                "message" : self.message,
                "ts": self.ts
            }
        else:
            dicionario = {
                "command" : self.command,
                "message" : self.message,
                "channel" : self.channel,
                "ts": self.ts
            }
        j=json.dumps(dicionario)
        return j


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        return RegisterMessage(username)


    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage(channel)

    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        return TextMessage(message,channel)


    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""
        
        msgJSON=bytes(msg.__str__(),encoding="utf-8")
        nrBytes=len(msgJSON).to_bytes(2,'big')
        if(len(msgJSON)>pow(2,16)):
            raise CDProtoBadFormat(msgJSON)
        connection.sendall(nrBytes+msgJSON) 


    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        compr=connection.recv(2)
        numero=int.from_bytes(compr,'big')
        msg=connection.recv(numero)
        msgJSON=msg.decode('utf-8')
        if(msgJSON!=""):
            try:
                msgString = json.loads(msgJSON)
                
            except json.decoder.JSONDecodeError:
                raise CDProtoBadFormat(msgJSON)

            if(msgString["command"]=="register"):
                return CDProto.register(msgString["user"])
            elif(msgString["command"]=="join"):
                return CDProto.join(msgString["channel"])
            elif(msgString["command"]=="message"):
                if("channel" in msgString):
                    return CDProto.message(msgString["message"],msgString["channel"])
                else:
                    return CDProto.message(msgString["message"])


class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
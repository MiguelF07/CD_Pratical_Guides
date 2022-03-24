"""Middleware to communicate with PubSub Message Broker."""
# The middleware is used by the producers and consumers simultaneously and it is the only one that interacts with the broker
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from sys import exec_prefix
import xml.etree.ElementTree as ET
import pickle
import socket
import time
import json 
from typing import Any

class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""


    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self._type =_type

        # Tratamento da socket
        self._host = "localhost"
        self._port = 5000
        self._address = (self._host, self._port)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect(self._address)
        self._socket.setblocking(True)
        

    # Função usada pelo Producer
    def push(self, value):
        """Sends data to broker. """
        pass

    def pull(self):
        """Waits for (topic, data) from broker.

        Should BLOCK the consumer!"""
        pass

    # Função usada pelo Consumer
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        pass


    # Função usada pelo Consumer
    def cancel(self):
        """Cancel subscription."""
        pass

class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        # Create JSON Queue.
        super().__init__(topic, _type)
        if _type == MiddlewareType.CONSUMER:
            formatMessage = {
                "command":"format",
                "message_format":"json"
            }
            messageData = json.dumps(formatMessage)
            m_bytes = bytes(messageData,encoding="utf-8")
            l_bytes = len(m_bytes).to_bytes(2,'big') 
            
            try:
                self._socket.send(l_bytes+m_bytes)
            except:pass
            
            subMessage = {
                "command": "subscribe",
                "topic": (topic)
            }
            subMessage = json.dumps(subMessage)
            m_bytes = bytes(subMessage,encoding="utf-8")
            l_bytes = len(m_bytes).to_bytes(2,'big')
            
            try:
                self._socket.send(l_bytes+m_bytes)
            except:pass
        else:
            formatMessage = {
                "command":"format",
                "message_format":"json"
            }
            messageData = json.dumps(formatMessage)
            m_bytes = bytes(messageData,encoding="utf-8")
            l_bytes = len(m_bytes).to_bytes(2,'big') 
            try:
                self._socket.send(l_bytes+m_bytes)
            except:pass
        

    # Função usada pelo Producer
    def push(self, value):
        """Sends data to broker. """
        # Aqui o produtor vai enviar a mensagem para o broker em formato JSON  
        message = {
            "command":"publish",
            "topic":(self.topic),
            "message":(value)
        }
        messageData = json.dumps(message)
        m_bytes = bytes(messageData,encoding="utf-8")
        l_bytes = len(m_bytes).to_bytes(2,'big') 
        try:
            self._socket.send(l_bytes+m_bytes)
        except:pass

    # Função usada pelo Consumer
    def pull(self):
        """Receives (topic, data) from broker.
        Should BLOCK the consumer!"""
        # O consumidor vai tentar receber uma mensagem do broker, se não tiver nada para receber, o receive bloqueia e por sua vez o
        # consumidor também
        try:
            dataLengthBytes = self._socket.recv(2)
            dataLength = int.from_bytes(dataLengthBytes,'big')
            data = self._socket.recv(dataLength)
            if data:
                new_data = data.decode('utf-8')
        except:
            pass
        else:
            if new_data:
                try:
                    msg = json.loads(new_data)
                    if msg["command"]=="publish":
                        return ((msg["topic"]),((msg["message"])))
                except:return None,None
            else:
                return None,None

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        # TODO: enviar mensagem de listing request
        message = {
            "command": "list"
        }
        json_msg = json.dumps(message)
        m_bytes = bytes(json_msg,encoding="utf-8")
        l_bytes = len(m_bytes).to_bytes(2,'big') 
        try:
            self._socket.send(l_bytes+m_bytes)
        except:pass

        try:
            dataLengthBytes = self._socket.recv(2)
            dataLength = int.from_bytes(dataLengthBytes,'big')
            data = self._socket.recv(dataLength)
            new_data = data.decode('utf-8')
        except:
            pass
        else:
            if new_data:
                msg = json.loads(new_data)
                if msg["command"]=="listRep":
                    return msg["message"]


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        # Create XML Queue.
        super().__init__(topic, _type)
        
        if _type == MiddlewareType.CONSUMER:
            formatMessage = {
                "command":"format",
                "message_format":"xml"
            }
            messageData = json.dumps(formatMessage)
            m_bytes = bytes(messageData,encoding="utf-8")
            l_bytes = len(m_bytes).to_bytes(2,'big')
            try:
                self._socket.send(l_bytes+m_bytes)
            except:pass
            
            subMessage = {
                "command": "subscribe",
                "topic": str(topic)
            }
            elem = ET.Element("subMessage", attrib=subMessage)
            encoded_msg = ET.tostring(elem)
            m_bytes = encoded_msg
            l_bytes = len(m_bytes).to_bytes(2,'big')
            try:
                self._socket.send(l_bytes+m_bytes)
            except:pass
        else:
            formatMessage = {
                "command":"format",
                "message_format":"xml"
            }
            messageData = json.dumps(formatMessage)
            m_bytes = bytes(messageData,encoding="utf-8")
            l_bytes = len(m_bytes).to_bytes(2,'big')
            
            try:
                self._socket.send(l_bytes+m_bytes)
            except:pass
        
    
     # Função usada pelo Producer
    def push(self, value):
        """Sends data to broker. """
        # Aqui o produtor vai enviar a mensagem para o broker no formato XML
        message = {
            "command":"publish",
            "topic":str(self.topic),
            "message":str(value)
        }
        elem = ET.Element("message", attrib=message)
        encoded_msg = ET.tostring(elem)
        m_bytes = encoded_msg
        l_bytes = len(m_bytes).to_bytes(2,'big')
        try:
            self._socket.send(l_bytes+m_bytes)
        except:pass

    # Função usada pelo Consumer
    def pull(self):
        """Receives (topic, data) from broker.
        Should BLOCK the consumer!"""
        # O consumidor vai tentar receber uma mensagem do broker, se não tiver nada para receber, o receive bloqueia e por sua vez o
        # consumidor também
        try:
            dataLengthBytes = self._socket.recv(2)
            dataLength = int.from_bytes(dataLengthBytes,'big')
            data = self._socket.recv(dataLength)
            if data:
                new_data = data.decode('utf-8')
        
        except:
            pass
        else:
            if new_data:
                try:
                    my_xml = ET.fromstring(new_data)
                    msg = my_xml.attrib
                    if msg["command"] =="publish":
                        return ((msg["topic"]),((msg["message"])))
                except:
                    return None,None
            else:
                pass


    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        # TODO: enviar mensagem de listing request
        message = {
            "command": "list"
        }
        elem = ET.Element("message", attrib=message)
        encoded_msg = ET.tostring(elem)
        m_bytes = encoded_msg
        l_bytes = len(m_bytes).to_bytes(2,'big')
        try:
            self._socket.send(l_bytes+m_bytes)
        except:pass

        try:
            dataLengthBytes = self._socket.recv(2)
            dataLength = int.from_bytes(dataLengthBytes)
            data = self._socket.recv(dataLength)
            if data:
                new_data=data.decode('utf-8')
                my_xml = ET.fromstring(new_data)
                msg = my_xml.attrib
                if msg["command"]=="listRep":
                        return msg["message"]
        except:
            pass
       
class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        # Create Pickle Queue.
        super().__init__(topic, _type)

        if _type == MiddlewareType.CONSUMER:
            formatMessage = {
                "command":"format",
                "message_format":"pickle"
            }
            messageData = json.dumps(formatMessage)
            m_bytes = bytes(messageData,encoding="utf-8")
            l_bytes = len(m_bytes).to_bytes(2,'big')
            try:
                self._socket.send(l_bytes+m_bytes)
            except:pass

            subMessage = {
                "command": "subscribe",
                "topic": (topic)
            }
            messageData = pickle.dumps(subMessage) 
            l_bytes = len(messageData).to_bytes(2,'big')
            try:
                self._socket.send(l_bytes+messageData)
            except:pass
        else:
            formatMessage = {
                "command":"format",
                "message_format":"pickle"
            }
            messageData = json.dumps(formatMessage)
            m_bytes = bytes(messageData,encoding="utf-8")
            l_bytes = len(m_bytes).to_bytes(2,'big')
            
            try:
                self._socket.send(l_bytes+m_bytes)
            except:pass
        
    
    # Função usada pelo Producer
    def push(self, value):
        """Sends data to broker. """
        # Aqui o produtor vai enviar a mensagem para o broker no formato Pickle
        message = {
            "command":"publish",
            "topic":(self.topic),
            "message":(value)
        }
        pickle_msg = pickle.dumps(message)
        l_bytes = len(pickle_msg).to_bytes(2,'big')
        try:
            self._socket.send(l_bytes+pickle_msg)
        except:pass

    # Função usada pelo Consumer
    def pull(self):
        """Receives (topic, data) from broker.
        Should BLOCK the consumer!"""
        # O consumidor vai tentar receber uma mensagem do broker, se não tiver nada para receber, o receive bloqueia e por sua vez o
        # consumidor também
        try:
            dataLengthBytes = self._socket.recv(2)
            dataLength = int.from_bytes(dataLengthBytes,'big')
            data = self._socket.recv(dataLength)
        except:
            pass
        else:
            if data:
                try:
                    msg = pickle.loads(data)
                    if msg["command"]=="publish":
                        return ((msg["topic"]),((msg["message"])))
                except:return None,None
            else:
                return None,None
                
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        # TODO: enviar mensagem de listing request
        message = {
            "command": "list"
        }
        pickle_msg = pickle.dumps(message)
        l_bytes = len(pickle_msg).to_bytes(2,'big')
        try:
            self._socket.send(l_bytes+pickle_msg)
        except:pass

        try:
            dataLengthBytes = self._socket.recv(2)
            dataLength = int.from_bytes(dataLengthBytes,'big')
            data = self._socket.recv(dataLength)
        except:
            pass
        else:
            if data:
                msg = pickle.loads(data)
                if msg["command"]=="listRep":
                    return msg["message"]

        

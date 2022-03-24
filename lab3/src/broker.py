"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import socket
import selectors
import json
import pickle
import xml.etree.ElementTree as ET


class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000

        # Tratamento da socket
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind((self._host, self._port)) 
        self._socket.listen(100)
        #conn, addr = self._socket.accept() 

        #Selectors
        self.sel = selectors.DefaultSelector()
        self.sel.register(self._socket, selectors.EVENT_READ, self.accept)

        # Dicionário que armazena chave:topico, valor: lista de subscritores
        self.topic_subscribers = {}
        # Dicionário em que a chave:tópico, valor:conteúdo do tópico
        self.topics = {}
        # Chave: socket do client, valor: Format de serialization
        self.clients = {} 
   


    def read(self, conn):

        try:
            dataLengthBytes = conn.recv(2)
            dataLength = int.from_bytes(dataLengthBytes,'big')
            data = conn.recv(dataLength)
        except:pass
        else:
            if data:
                if conn not in self.clients.keys():
                    new_data=data.decode('utf-8')
                    format_msg = json.loads(new_data)
                    if format_msg["message_format"] == "json":
                        self.clients[conn] = Serializer.JSON.value
                    elif format_msg["message_format"] == "xml":
                        self.clients[conn] = Serializer.XML.value
                    elif format_msg["message_format"] == "pickle":
                        self.clients[conn] = Serializer.PICKLE.value
                else:
                    if self.clients[conn] == Serializer.JSON.value:
                        new_data=data.decode('utf-8')
                        loaded_msg = json.loads(new_data)

                        if loaded_msg["command"]=="subscribe":
                            self.subscribe(loaded_msg["topic"],conn,Serializer.JSON.value)
                            if loaded_msg["topic"] in self.topics.keys():
                                msg = {
                                    "command":"publish",
                                    "topic": (loaded_msg["topic"]),
                                    "message": (self.topics[loaded_msg["topic"]])
                                }
                                messageData = json.dumps(msg)
                                m_bytes = bytes(messageData,encoding="utf-8")
                                l_bytes = len(messageData).to_bytes(2,'big')
                                try:
                                    conn.send(l_bytes+m_bytes)
                                except:pass

                            for topic in self.topics.keys():
                                if topic.startswith(loaded_msg["topic"]):
                                    if loaded_msg["topic"]!=topic:
                                        for (address, _format) in self.topic_subscribers[topic]:
                                            self.subscribe(topic,address,Serializer.JSON.value)
                                        if loaded_msg["topic"] in self.topics.keys():
                                            msg = {
                                                "command":"publish",
                                                "topic": (loaded_msg["topic"]),
                                                "message": (self.topics[loaded_msg["topic"]])
                                            }
                                            messageData = json.dumps(msg)
                                            m_bytes = bytes(messageData,encoding="utf-8")
                                            l_bytes = len(messageData).to_bytes(2,'big')
                                            try:
                                                address.send(l_bytes+m_bytes)
                                            except:pass
                                    
                        elif loaded_msg["command"] == "publish":
                            self.put_topic(loaded_msg["topic"],loaded_msg["message"])
                            self.sendPublish(loaded_msg)
                                
                        elif loaded_msg["command"]=="list":
                            list_top = self.list_topics()
                            msg= {
                                "command":"listRep",
                                "message": (list_top)
                                }
                            messageData = json.dumps(msg)
                            m_bytes = bytes(messageData,encoding="utf-8")
                            l_bytes = len(m_bytes).to_bytes(2,'big')
                            try:
                                conn.send(l_bytes+m_bytes)
                            except:pass

                        elif loaded_msg["command"] == "cancel":
                            self.unsubscribe(loaded_msg["topic"], conn)              

                    elif self.clients[conn] == Serializer.XML.value:
                        new_data=data.decode('utf-8')
                        my_xml = ET.fromstring(new_data)
                        loaded_msg = my_xml.attrib
                   

                        if loaded_msg["command"]=="subscribe":
                            self.subscribe(loaded_msg["topic"],conn,Serializer.XML.value)

                            for topic in self.topics.keys():
                                if topic.startswith(loaded_msg["topic"]) and loaded_msg["topic"]!=topic:
                                    self.subscribe(topic,conn,Serializer.XML.value)


                            if loaded_msg["topic"] in self.topics.keys():
                                msg = {
                                    "command":"publish",
                                    "topic": str((loaded_msg["topic"])),
                                    "message": str((self.topics[loaded_msg["topic"]]))
                                }
                                elem = ET.Element("loaded_msg", attrib=msg)
                                encoded_msg = ET.tostring(elem)
                                m_bytes = encoded_msg
                                l_bytes = len(m_bytes).to_bytes(2,'big')
                                try:
                                    conn.send(l_bytes+m_bytes)
                                except:pass

                        elif loaded_msg["command"] == "publish":
                            self.put_topic(loaded_msg["topic"],loaded_msg["message"])
                            self.sendPublish(loaded_msg)

                                
                        elif loaded_msg["command"]=="list":
                            list_top = self.list_topics()
                            msg= {
                                    "command":"listRep",
                                    "message": list_top
                                    }
                            elem = ET.Element("loaded_msg", attrib=msg)
                            encoded_msg = ET.tostring(elem)
                            m_bytes = encoded_msg
                            l_bytes = len(m_bytes).to_bytes(2,'big')
                            try:
                                conn.send(l_bytes+m_bytes)
                            except:pass

                        elif loaded_msg["command"] == "cancel":
                            self.unsubscribe(loaded_msg["topic"], conn)     

                    elif self.clients[conn] == Serializer.PICKLE.value:
                        try:
                            loaded_msg = pickle.loads(data)

                            if loaded_msg["command"]=="subscribe":
                                self.subscribe(loaded_msg["topic"],conn,Serializer.PICKLE.value)

                                for topic in self.topics.keys():
                                    if topic.startswith(loaded_msg["topic"]) and loaded_msg["topic"]!=topic:
                                        self.subscribe(topic,conn,Serializer.PICKLE.value)

                                if loaded_msg["topic"] in self.topics.keys():
                                    msg = {
                                        "command":"publish",
                                        "topic": (loaded_msg["topic"]),
                                        "message": (self.topics[loaded_msg["topic"]])
                                    }
                                    messageData = pickle.dumps(msg)
                                    l_bytes = len(messageData).to_bytes(2,'big')
                                    try:
                                        conn.send(l_bytes+messageData)
                                    except:pass

                            elif loaded_msg["command"] == "publish":
                                self.put_topic(loaded_msg["topic"],loaded_msg["message"])
                                self.sendPublish(loaded_msg)
                                
                            elif loaded_msg["command"]=="list":
                                list_top = self.list_topics()
                                msg= {
                                    "command":"listRep",
                                    "message": list_top
                                    }
                                messageData = pickle.dumps(msg)
                                l_bytes = len(messageData).to_bytes(2,'big')
                                try:
                                    conn.send(l_bytes+messageData)
                                except:pass

                            elif loaded_msg["command"] == "cancel":
                                self.unsubscribe(loaded_msg["topic"], conn)
                        except:pass
                    else:
                        return None,None
    
    def sendPublish(self, loaded_msg: dict):
        for topic in self.topic_subscribers.keys():
            if (loaded_msg["topic"]).startswith(topic):
                for client,_format in self.topic_subscribers[topic]:
                    if _format == 0:
                        messageData = json.dumps(loaded_msg)
                        m_bytes = bytes(messageData,encoding="utf-8")

                    elif _format == 1:
                        elem = ET.Element("loaded_msg", attrib={"command":loaded_msg["command"],"topic":str(loaded_msg["topic"]),"message":str(loaded_msg["message"])})
                        encoded_msg = ET.tostring(elem)
                        m_bytes = encoded_msg
                    
                    elif _format == 2:
                        m_bytes = pickle.dumps(loaded_msg)
                        
                    
                    try:
                        l_bytes = len(m_bytes).to_bytes(2,'big')
                        client.send(l_bytes+m_bytes)
                    except:pass

    
    def accept(self, _socket):
        conn, addr = self._socket.accept() 
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)


    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        return list(self.topics.keys())
 

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.topics:
            return (self.topics.get(topic))

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topics[topic] = value

    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        return self.topic_subscribers.get(topic)

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if(topic in self.topic_subscribers.keys()):
            self.topic_subscribers[topic].append((address, _format))
        else:
            self.topic_subscribers[topic]=[]
            self.topic_subscribers[topic].append((address, _format))    
        self.clients[address]=_format    

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        if(topic in self.topic_subscribers.keys()):
            self.topic_subscribers[topic].remove((address,self.clients[address]))


    def run(self):
        """Run until canceled."""
        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj)
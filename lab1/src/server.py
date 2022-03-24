"""CD Chat server program."""
import logging
import socket
import selectors
import json

from .protocol import CDProto, CDProtoBadFormat

logging.basicConfig(filename="server.log", level=logging.DEBUG)


class Server:
    """Chat Server process."""
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('localhost', 1234))
        self.sock.listen(100)
        self.clients={}
        self.sel = selectors.DefaultSelector()


    def loop(self):
        """Loop indefinetely."""
        
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)
        while True:
            events = self.sel.select() 
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask) 

    def accept(self,sock, mask):
        conn, addr = self.sock.accept() 
        self.clients[conn] = []
        self.clients[conn].append(None)
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)

    def read(self,conn,mask):
        
        data = CDProto.recv_msg(conn)
        if data:
            logging.debug('received "%s', data.__str__())
            if(data.command=="join"):
                if(data.channel in self.clients[conn]):
                    self.clients[conn].remove(data.channel)
                    self.clients[conn].append(data.channel)
                else:
                    self.clients[conn].append(data.channel)
            elif(data.command=="message"):
                channel = self.clients[conn][len(self.clients[conn])-1]
                if(channel==None):
                    for i in self.clients:
                        if(None in self.clients[i]):
                            CDProto.send_msg(i,data)
                            logging.debug('sent "%s', data)
                else:
                    for client in self.clients:
                        if(channel in self.clients[client]):
                            CDProto.send_msg(client,data)
                            logging.debug('sent "%s', data)
        else:
            print('closing', conn)
            self.sel.unregister(conn)
            conn.close()
            del self.clients[conn]

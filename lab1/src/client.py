"""CD Chat client program"""
import logging
import socket
import sys
import selectors
import json
import fcntl
import os

from .protocol import CDProto, CDProtoBadFormat

logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)

# set sys.stdin non-blocking
orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)


class Client:
    """Chat Client process."""

    def __init__(self, name: str = "Foo"):
        """Initializes chat client."""
        self.name = name
        self.sock = socket.socket()
        self.sel = selectors.DefaultSelector()
    

    def connect(self):
        """Connect to chat server and setup stdin flags."""

        self.sock.connect(('localhost',1234))

        rgMsg=CDProto.register(self.name)
        CDProto.send_msg(self.sock,rgMsg)
        logging.debug('sent "%s', rgMsg.__str__())
        self.sel.register(sys.stdin, selectors.EVENT_READ, self.got_keyboard_data)
        self.sel.register(self.sock, selectors.EVENT_READ, self.read)

    def got_keyboard_data(self,stdin):
        texto = stdin.read()
        texto=texto.rstrip()
        if(texto.startswith("/join ")):
                canal=texto[6:len(texto)]
                mensagemJoin=CDProto.join(canal)
                CDProto.send_msg(self.sock,mensagemJoin)
                logging.debug('sent "%s', mensagemJoin.__str__())
        elif(texto=="exit"):
                self.sel.unregister(sys.stdin)
                self.sel.unregister(self.sock)
                self.sock.close()
                sys.exit(0)
        else:
                mensagem=CDProto.message(texto)
                CDProto.send_msg(self.sock,mensagem)
                logging.debug('sent "%s', mensagem.__str__())

    
    def read(self,conn):
        mensagemRecebida=CDProto.recv_msg(conn)
        if mensagemRecebida:
                logging.debug('received "%s', mensagemRecebida)
                print("\r"+mensagemRecebida.message)
        else:
            self.sel.unregister(conn)
            conn.close()

    def loop(self):
        """Loop indefinetely."""
        sys.stdout.write("\r>")
        sys.stdout.flush()
        while True:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj)

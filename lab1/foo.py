from src.client import Client

if __name__ == "__main__":
    c = Client("Foo")
    c.connect()
    
    c.loop()

# Echo client program
#import socket

#HOST = 'localhost'    # The remote host
#PORT = 50007              # The same port as used by the server
#with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #Cria-se uma socket exatamente com as mesmas propriedades
#    s.connect((HOST, PORT))
#    s.sendall(b'Hello, world') #Como o socket só é capaz de enviar bytes, coloca-se um b antes para informar que a string em causa é uma string de bytes, e não uma string Python-native
#    data = s.recv(1024) #Faço um recieve
#print('Received', repr(data)) #O repr converte automaticamente a string de bytes em string python

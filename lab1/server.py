from src.server import Server

if __name__ == "__main__":
   s = Server()

   s.loop()

# Echo server program
#import socket

#Variáveis globais são usadas em maiúsculas
#HOST = ''                 # Symbolic name meaning all available interfaces -> Endereço do servidor
#PORT = 50007              # Arbitrary non-privileged port
#Se sair fora do bloco with ele vai fazer automaticamente o close da socket
#with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#    s.bind((HOST, PORT)) #Endereço das Sockets
#    s.listen(1) #Diz ao sistema operativo que o servidor vai atender no máximo 1 cliente de cada vez
#    conn, addr = s.accept() #Linha que vai aceitar o cliente (bloqueia o processo até receber o processo)
#    with conn: #Quando recebe o cliente retorna uma nova socket (conn)
#        print('Connected by', addr)
#        while True: #Enquanto houver dados para receber desta socket
#            data = conn.recv(1024) #Até 1024 bytes são lidos de uma vez
#            if not data: break #Se ele não ler nada, a socket é terminada
#            conn.sendall(data) #Faz o envio da informação toda do data
            #Data é a mensagem que acabei de receber

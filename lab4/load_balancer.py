# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse
import time

# configure logger output format
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None

# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):  
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    def __init__(self, servers):
        self.servers = servers  

    def select_server(self):
        return self.servers[0]

    def update(self, *arg):
        pass


# round robin policy
class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.index = 0

    def select_server(self):
        if self.index == len(self.servers):
            ret = 0
            self.index = 1
            return self.servers[ret]
        else:
            ret = self.index
            self.index+=1
            return self.servers[ret]
    
    def update(self, *arg):
        pass


# least connections policy
class LeastConnections:
    def __init__(self, servers):
        self.servers = servers
        
        self.connections = {i:[] for i in servers}

    def select_server(self):
        counter=0
        for server in self.servers:
            if counter==0:
                less = len(self.connections[server])
                idxless = counter
            else:
                if len(self.connections[server])<less:
                    less=len(self.connections[server])
                    idxless = counter
            counter+=1
        return servers[idxless]


    def update(self, *arg):
        if arg[0] == 'add':
            self.connections[arg[1]].append(arg[2])
        elif arg[0] == 'del':
            # for i in self.connections.values():
            #     if arg[1] in i:
            #         self.connections[i].remove(arg[1])
            for key,value in self.connections.items():
                if arg[1] in value:
                    self.connections[key].remove(arg[1])


# least response time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers
        #Dicionario em que chave vai ser o server e o value vai ser o tempo desse server
        self.connections = {i:-1 for i in servers}
        self.temporaryTimes={i:-1 for i in servers}
        self.count=0;
        self.associates = {i:[] for i in servers}

    def select_server(self):
        if self.count < len(self.servers):
            self.count+=1
            return self.servers[self.count-1]
        else:
            valores = self.connections.values()
            minimo = min(valores)
            indice = list(valores).index(minimo)
            return self.servers[indice]

    def update(self, *arg):
        #if self.count<len(self.servers):
        if arg[0] == 'add':
            self.temporaryTimes[arg[1]]=arg[2]
            self.associates[arg[1]].append(arg[3])
        elif arg[0] == 'del':
            counterr=0
            for servers in self.associates.values():
                if arg[1] in servers:
                    idx=counterr
                    tempo = arg[2]-self.temporaryTimes[self.servers[idx]]
                    self.connections[self.servers[idx]]=tempo
                    break
                counterr+=1
            
            for key,value in self.associates.items():
                if arg[1] in value:
                    self.associates[key].remove(arg[1])




POLICIES = {
    "N2One": N2One,
    "RoundRobin": RoundRobin,
    "LeastConnections": LeastConnections,
    "LeastResponseTime": LeastResponseTime
}

class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}

    def add(self, client_sock, upstream_server):
        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        if type(self.policy) == LeastConnections:
            policy.update("add",upstream_server,client_sock)
        elif type(self.policy) == LeastResponseTime:
            policy.update("add",upstream_server,time.time(),client_sock)
        
        sel.register(upstream_sock, selectors.EVENT_READ, read)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] =  upstream_sock

    def delete(self, sock):
        if type(self.policy) == LeastConnections:
            policy.update("del",sock)
        elif type(self.policy) == LeastResponseTime:
            policy.update("del",sock,time.time())
        sel.unregister(sock)
        sock.close()
        if sock in self.map:
            self.map.pop(sock)
        

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ())) 

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    mapper.add(client, policy.select_server())

def read(conn,mask):
    data = conn.recv(4096)
    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        mapper.get_sock(conn).send(data)


def main(addr, servers, policy_class):
    global policy
    global mapper

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = policy_class(servers)
    mapper = SocketMapper(policy)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select(timeout=1)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
                
    except Exception as err:
        logger.error(err,exc_info=1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-a', dest='policy', choices=POLICIES)
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = [('localhost', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers, POLICIES[args.policy])

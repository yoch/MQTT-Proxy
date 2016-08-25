import socket
import select
import socketserver
from collections import deque
import mqtt_protocol as mqtt


class MQTTProxy:
    def __init__(self, client, broker):
        self.client = client
        self.broker = broker
        self.bqueue = deque()
        self.cqueue = deque()
    def run(self):
        fds = [self.client, self.broker]
        queues = [self.cqueue, self.bqueue]
        readers = fds[:]
        while True:
            writers = [fd for fd, q in zip(fds, queues) if q]
            if not readers and not writers:
                break
            rlist, wlist, _ = select.select(readers, writers, [])
            if self.client in rlist:
                data = mqtt.read_paquet(self.client)
                if data is not None:
                    self.bqueue.append(data)
                else:
                    readers.remove(self.client)
                #self.broker.sendall(data)
            if self.broker in rlist:
                data = mqtt.read_paquet(self.broker)
                if data is not None:
                    self.cqueue.append(data)
                else:
                    readers.remove(self.broker)
                #self.client.sendall(data)
            if self.client in wlist:
                data = self.cqueue.popleft()
                self.client.sendall(data)
            if self.broker in wlist:
                data = self.bqueue.popleft()
                self.broker.sendall(data)

            

class MQTTProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        print('handle connection from', self.client_address)
        sock = socket.create_connection(self.server.broker_address)
        cproxy = MQTTProxy(self.request, sock)
        cproxy.run()
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
    def finish(self):
        print('closing connection from', self.client_address)
        #self.request.shutdown(socket.SHUT_RDWR)
        #self.request.close()


class MQTTProxyServer(socketserver.ThreadingTCPServer):
    def __init__(self, local_address, broker_address):
        super().__init__(local_address, MQTTProxyHandler)
        self.broker_address = broker_address


HOST = '0.0.0.0'
PORT = 1883
BROKER_ADDR = 'broker.hivemq.com', 1883

#'broker.hivemq.com' 'iot.eclipse.org', 'test.mosquitto.org'

if __name__ == '__main__':
    proxy = MQTTProxyServer((HOST, PORT), BROKER_ADDR)
    proxy.serve_forever()

#!/usr/bin/env python
# -*- coding: utf8 -*-

import socket
import select
import socketserver
from collections import deque
import mqtt_protocol as mqtt


class MQTTConnectionHandler:
    """
    Manage two side connections between client and broker
    """

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
        # connect socket to broker
        sock = socket.create_connection(self.server.broker_address)
        handler = MQTTConnectionHandler(self.request, sock)
        handler.run()
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


# python3 mqttproxy.py -broker broker.hivemq.com:1883
# 'broker.hivemq.com' 'iot.eclipse.org', 'test.mosquitto.org'

if __name__ == '__main__':
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-broker', required=True, metavar='HOST:PORT')
    parser.add_argument('-host', default='0.0.0.0')
    parser.add_argument('-port', type=int, default=1883)

    args = parser.parse_args(sys.argv[1:])
    host, port = args.broker.split(':')
    broker_address = host, int(port)

    proxy = MQTTProxyServer((args.host, args.port), broker_address)
    proxy.serve_forever()

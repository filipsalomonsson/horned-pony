#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import socket
import select

from wsgiref.simple_server import demo_app, WSGIRequestHandler

class HornedServer(object):
    def __init__(self, app):
        self.app = app
        self.base_environ = {}

    def get_app(self):
        return self.app

    def listen(self):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', 6666))
        self.sock.listen(10)

    def serve_forever(self):
        try:
            while True:
                socks, _, _ = select.select([self.sock], [], [])
                for sock in socks:
                    connection, address = sock.accept()
                    print os.getpid()
                    handler = WSGIRequestHandler(connection,
                                                 address,
                                                 self)
                    connection.close()
        except:
            sys.exit(1)


if __name__ == '__main__':
    worker = HornedServer(demo_app)
    worker.listen()
    worker.serve_forever()


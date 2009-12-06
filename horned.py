#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import socket
import select

def demo_app(environ,start_response):
    start_response("200 OK", [('Content-Type','text/plain')])
    return ["Hello world!\n\n"]# + ["%s=%s\n" % item for item in sorted(environ.items())]

class WSGIRequestHandler(object):
    def __init__(self, application, server):
        self.application = application
        env = self.baseenv = os.environ.copy()
        host, port = server.sock.getsockname()[:2]
        env["SERVER_NAME"] = socket.getfqdn(host)
        env["SERVER_PORT"] = str(port)

    def __call__(self, connection, address):
        rfile = connection.makefile("rb", -1)
        wfile = connection.makefile("wb", 0)

        reqline = rfile.readline()[:-2]
        method, path, protocol = reqline.split(" ", 2)

        env = self.baseenv.copy()

        env["SERVER_PROTOCOL"] = protocol
        env["REQUEST_METHOD"] = method
        env["SCRIPT_NAME"] = path
        env["PATH_INFO"] = path
        env["REMOTE_ADDR"] = address[0]

        headers = {}
        while True:
            line = rfile.readline().rstrip("\r\n")
            if line == "":
                break
            key, value = line.split(":", 1)
            headers[key.lower()] = value

            for key, value in headers.iteritems():
                env["HTTP_" + key.replace("-", "_").upper()] = value

        def start_response(status, response_headers, exc_info=None):
            wfile.write('HTTP/1.1 %s\r\n' % status)
            for header in response_headers:
                wfile.write('%s: %s\r\n' % header)
                wfile.write('\r\n')
            return wfile.write 

        response_data = self.application(env, start_response)

        for chunk in response_data:
            wfile.write(chunk)
        if hasattr(response_data, "close"):
            response_data.close()

        rfile.close()
        wfile.close()

        # PATH_INFO
        # QUERY_STRING?
        # CONTENT_TYPE?
        # CONTENT_LENGTH?
        # HTTP_*

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
        handler = WSGIRequestHandler(demo_app, self)

        try:
            while True:
                socks, _, _ = select.select([self.sock], [], [])
                for sock in socks:
                    connection, address = sock.accept()
                    handler(connection, address)
                    connection.close()
        except KeyboardInterrupt:
            sys.exit(1)


if __name__ == '__main__':
    worker = HornedServer(demo_app)
    worker.listen()
    worker.serve_forever()


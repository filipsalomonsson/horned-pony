#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import socket
import select

def demo_app(environ,start_response):
    start_response("200 OK", [('Content-Type','text/plain')])
    return ["Hello world!\n\n"]# + ["%s=%s\n" % item for item in sorted(environ.items())]

class HTTPResponse(object):
    def __init__(self, connection, address):
        self.wfile = connection.makefile("wb", 0)
        self.status = None
        self.headers = []
        self.headers_sent = False

    def write(self, data):
        write = self.wfile.write
        if not self.headers_sent:
            write('HTTP/1.1 %s\r\n' % self.status)
            for header in self.headers:
                write('%s: %s\r\n' % header)
            write('\r\n')
            self.headers_sent = True
        write(data)

    def start_response(self, status, response_headers, exc_info=None):
        self.status = status
        self.headers = response_headers
        return self.write

    def send(self, data):
        write = self.write
        for chunk in data:
            write(chunk)
        if not self.headers_sent:
            write("")
        if hasattr(data, "close"):
            response_data.close()
        self.wfile.close()


class WSGIRequestHandler(object):
    def __init__(self, application, server):
        self.application = application
        env = self.baseenv = os.environ.copy()
        host, port = server.sock.getsockname()[:2]
        env["SERVER_NAME"] = socket.getfqdn(host)
        env["SERVER_PORT"] = str(port)

    def __call__(self, connection, address):
        rfile = connection.makefile("rb", -1)

        reqline = rfile.readline()[:-2]
        method, path, protocol = reqline.split(" ", 2)

        env = self.baseenv.copy()

        env["SERVER_PROTOCOL"] = protocol
        env["REQUEST_METHOD"] = method
        env["SCRIPT_NAME"] = path
        env["PATH_INFO"] = path
        env["REMOTE_ADDR"] = address[0]

        env["wsgi.version"] = (1, 0)
        env["wsgi.url_scheme"] = "http"
        env["wsgi.input"] = rfile
        env["wsgi.errors"] = sys.stderr
        env["wsgi.multithread"] = False
        env["wsgi.multiprocess"] = True
        env["wsgi.run_once"] = False

        headers = {}
        for line in rfile:
            line = line[:-2]
            if not line:
                break
            key, _, value = line.partition(":")
            headers[key] = value.strip()

            for key, value in headers.iteritems():
                key = key.replace("-", "_").upper()
                value = value.strip()
                env["HTTP_" + key] = value

        response = HTTPResponse(connection, address)
        response.send(self.application(env, response.start_response))

        rfile.close()


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


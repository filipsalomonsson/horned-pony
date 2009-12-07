#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import socket
import select
import signal
import errno
import urllib

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
        if exc_info is not None:
            try:
                if self.headers_sent:
                    raise exc_info[0], exc_info[1], exc_info[2]
            finally:
                exc_info = None
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
        env["REMOTE_ADDR"] = address[0]
        env["SCRIPT_NAME"] = path
        if "?" in path:
            path, _, query = path.partition("?")
            env["QUERY_STRING"] = query
        env["PATH_INFO"] = urllib.unquote(path)

        env["wsgi.version"] = (1, 0)
        env["wsgi.url_scheme"] = "http"
        env["wsgi.input"] = rfile
        env["wsgi.errors"] = sys.stderr
        env["wsgi.multithread"] = False
        env["wsgi.multiprocess"] = True
        env["wsgi.run_once"] = False

        for line in rfile:
            line = line[:-2]
            if not line:
                break
            key, _, value = line.partition(":")
            key = key.replace("-", "_").upper()
            value = value.strip()
            env["HTTP_" + key] = value

        response = HTTPResponse(connection, address)
        response.send(self.application(env, response.start_response))

        rfile.close()


class HornedManager(object):
    def __init__(self, app, workers=3):
        self.app = app
        self.workers = workers
        self.base_environ = {}
        self.worker_pids = set()
        self.alive = True

        signal.signal(signal.SIGINT, self.die_gracefully)

    def listen(self):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', 6666))
        self.sock.listen(50)

    def serve_forever(self):
        while self.alive:
            self.cleanup_workers()
            self.spawn_workers()
            time.sleep(1)
        for pid in self.worker_pids:
            os.kill(pid, signal.SIGINT)

    def cleanup_workers(self):
        for pid in list(self.worker_pids):
            pid, status = os.waitpid(pid, os.WNOHANG)
            if pid:
                self.worker_pids.remove(pid)

    def spawn_workers(self):
        while len(self.worker_pids) < self.workers:
            worker_pid = os.fork()
            if worker_pid:
                self.worker_pids.add(worker_pid)
            else:
                worker = HornedWorker(self.sock, self.app)
                worker.serve_forever()

    def die_gracefully(self, signum, frame):
        self.alive = False


class HornedWorker(object):
    def __init__(self, sock, app):
        self.sock = sock
        self.app = app
        self.alive = True

        self.rpipe, self.wpipe = os.pipe()

        signal.signal(signal.SIGINT, self.die_gracefully)

    def serve_forever(self):
        handler = WSGIRequestHandler(self.app, self)
        while self.alive:
            try:
                socks, _, _ = select.select([self.sock, self.rpipe], [], [])
            except select.error, e:
                if e[0] == errno.EINTR:
                    continue
            for sock in socks:
                connection, address = sock.accept()
                try:
                    handler(connection, address)
                except socket.error, e:
                    if e[0] == errno.EPIPE:
                        pass
                connection.close()
        sys.exit(0)
            
    def die_gracefully(self, signum, frame):
        self.alive = False
        os.write(self.wpipe, ".")

if __name__ == '__main__':
    worker = HornedManager(demo_app)
    worker.listen()
    worker.serve_forever()


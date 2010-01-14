#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2009 Filip Salomonsson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import os
import sys
import time
import _socket as socket
import select
import signal
import errno
import struct


class Logfile(object):
    def __init__(self, filename):
        if isinstance(filename, basestring):
            self.filename = filename
        else:
            self.filename = None
            self.file = filename
        self.reopen()

    def write(self, data):
        self.file.write(data)

    def flush(self):
        self.file.flush()

    def reopen(self):
        if not self.filename:
            return False
        try:
            new_file = open(self.filename, "a", 0)
        except:
            return False
        else:
            self.file = new_file
            return True


DEBUG, INFO, ERROR = 1, 2, 3
class Logger(object):
    def __init__(self, stdout=sys.stdout, stderr=sys.stderr, level=INFO):
        self.stdout = Logfile(stdout)
        self.stderr = Logfile(stderr)
        self.level = level

    def reopen(self, *args, **kwargs):
        self.info("Reopening log files", pid=True)
        if not self.stdout.reopen():
            self.error("Could not reopen stdout", pid=True)
        if not self.stderr.reopen():
            self.error("Could not reopen stderr", pid=True)

    def error(self, msg, *args, **kwargs):
        if self.level <= ERROR:
            self.write("error", msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        if self.level <= INFO:
            self.write("info", msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        if self.level <= DEBUG:
            self.write("debug", msg, *args, **kwargs)

    def request(self, client, request, status, length, reqtime=None):
        now = time.gmtime()
        timestamp = time.strftime("%m/%%s/%Y:%H:%M:%S +0000", now)
        timestamp = timestamp % (HTTP_MONTH[now[1]])
        line = ('%s - - [%s] "%s" %s %d "-" "-"\n'
                % (client, timestamp, request, status, length))
        self.stdout.write(line)
        self.stdout.flush()

    def write(self, level, msg, *args, **kwargs):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        if "pid" in kwargs:
            msg = "(#%d) %s" % (os.getpid(), msg)
        line = "%s %s\n" % (timestamp, msg % args)
        self.stderr.write(line)
        self.stderr.flush()

log = Logger()

charfromhex = {}
for i in xrange(256):
    charfromhex["%02x" % i] = charfromhex["%02X" % i] = chr(i)

def urlunquote(quoted):
    unquoted = ""
    while "%" in quoted:
        before, _, after = quoted.partition("%")
        code, quoted = after[:2], after[2:]
        unquoted += before + charfromhex.get(code, "%" + code)
    unquoted += quoted
    return unquoted

def demo_app(environ,start_response):
    start_response("200 OK", [('Content-Type','text/html')])
    return ["<html><head><title>Hello world!</title></head>"
            "<body><h1>Hello world!</h1></body></html>\n\n"]

HTTP_WDAY = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
HTTP_MONTH = (None, "Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

def http_date(timestamp=None):
    timestamp = timestamp or time.time()
    (year, month, day, hour, minute, second,
     weekday, yearday, isdst) = time.gmtime(timestamp)
    return "%s, %02d %3s %4d %02d:%02d:%02d GMT" % \
        (HTTP_WDAY[weekday], day, HTTP_MONTH[month], year,
         hour, minute, second)


class IOStream(object):
    def __init__(self, socket):
        self.socket = socket
        self.read_buffer = ""
        self.write_buffer = ""

    def read(self, size=-1):
        if size < 0:
            while True:
                chunk = self.socket.recv(4096)
                if not chunk:
                    break
                self.read_buffer += chunk
            result = self.read_buffer
            self.read_buffer = ""
            return result
        else:
            while len(self.read_buffer) < size:
                chunk = self.socket.recv(4096)
                if not chunk:
                    break
            result = self.read_buffer[:size]
            self.read_buffer = self.read_buffer[size:]
            return result

    def read_until(self, delimiter):
        while delimiter not in self.read_buffer:
            chunk = self.socket.recv(4096)
            if not chunk:
                break
            self.read_buffer += chunk
        index = self.read_buffer.find(delimiter)
        if not index > 0:
            raise ValueError()
        result = self.read_buffer[:index+len(delimiter)]
        self.read_buffer = self.read_buffer[index+len(delimiter):]
        return result

    def readline(self):
        try:
            return self.read_until("\n")
        except ValueError:
            return ""

    def readlines(self):
        return list(self)

    def write(self, data):
        self.write_buffer += data

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def flush(self):
        self.socket.sendall(self.write_buffer)
        self.write_buffer = ""

    def close(self):
        self.flush()

    def __iter__(self):
        return self

    def next(self):
        line = self.readline()
        if not line:
            raise StopIteration
        return line


def get_app(name):
    module_name, _, app_name = name.rpartition(".")
    module = __import__(module_name)
    for part in module_name.split(".")[1:]:
        module = getattr(module, part)
    app = getattr(module, app_name)
    return app

DEFAULT_CONFIG = dict(app=demo_app,
                      address=("127.0.0.1", 8080),
                      worker_processes=4,
                      access_log=sys.stdout,
                      error_log=sys.stderr)

class HornedManager(object):
    def __init__(self, config):
        self.config = DEFAULT_CONFIG.copy()
        self.config.update(config)

        self.worker_processes = self.config.get("worker_processes")
        self.app = self.config.get("app")

        global log
        log = Logger(self.config.get("access_log"),
                     self.config.get("error_log"))

        self.base_environ = {}
        self.workers = set()
        self.alive = True

        signal.signal(signal.SIGQUIT, self.die_gracefully)
        signal.signal(signal.SIGINT, self.die_immediately)
        signal.signal(signal.SIGTERM, self.die_immediately)
        signal.signal(signal.SIGUSR1, log.reopen)

    def listen(self, address):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(address)
        self.sock.listen(1024)

    def serve_forever(self):
        log.info("Starting manager...", pid=True)
        self.listen(self.config.get("address"))
        log.info("Fired up; ready to go!", pid=True)
        while self.alive:
            self.cleanup_workers()
            self.spawn_workers()
            time.sleep(1)
        log.info("Reaping workers...", pid=True)
        for worker in self.workers:
            worker.die_gracefully()
        t = time.time()
        while self.workers:
            if time.time() - t > 10:
                log.error("%d children won't die.",
                              len(self.children), pid=True)
                break
            for worker in list(self.workers):
                pid, status = worker.wait(os.WNOHANG)
                if pid:
                    self.workers.remove(worker)
            time.sleep(0.1)
        log.info("Manager done. Exiting.", pid=True)

    def cleanup_workers(self):
        for worker in list(self.workers):
            pid, status = os.waitpid(worker.pid, os.WNOHANG)
            if pid:
                self.workers.remove(worker)
                log.info("Worker #%d died." % pid, pid=True)

    def spawn_workers(self):
        while len(self.workers) < self.worker_processes:
            worker = HornedWorker(self.sock, self.app)
            self.workers.add(worker)
            worker.run()

    def die_gracefully(self, signum, frame):
        log.info("Manager shutting down gracefully...", pid=True)
        self.alive = False

    def die_immediately(self, signum, frame):
        log.info("Immediate death requested...")
        for worker in list(self.workers):
            worker.die_immediately()
        log.info("Bye.")
        sys.exit(0)


class HornedWorker:
    def __init__(self, sock, app):
        self.sock = sock
        self.app = app
        self.pid = None
        self.timestamp = int(time.time())
        self.requests = self.errors = 0

    def run(self):
        pid = os.fork()
        if pid:
            log.info("Spawned worker #%d." % pid, pid=True)
            self.pid = pid
        else:
            HornedWorkerProcess(self.sock, self.app).serve_forever()

    def die_gracefully(self):
        log.info("Sending SIGQUIT to worker #%d" % self.pid, pid=True)
        os.kill(self.pid, signal.SIGQUIT)

    def die_immediately(self):
        log.info("Sending SIGTERM to worker #%d" % self.pid, pid=True)
        os.kill(self.pid, signal.SIGTERM)

    def wait(self, *options):
        return os.waitpid(self.pid, *options)


class HornedWorkerProcess(object):
    def __init__(self, sock, app):
        self.sock = sock
        self.app = app
        self.alive = True
        self.requests = 0
        self.errors = 0
        self.rpipe, self.wpipe = os.pipe()

        env = self.baseenv = os.environ.copy()
        host, port = sock.getsockname()[:2]
        env.update({"SERVER_NAME": socket.gethostname(),
                    "SERVER_PORT": str(port), 
                    "SCRIPT_NAME": "", 
                    "wsgi.version": (1, 0), 
                    "wsgi.url_scheme": "http", 
                    "wsgi.errors": sys.stderr, 
                    "wsgi.multithread": False, 
                    "wsgi.multiprocess": True, 
                    "wsgi.run_once": False})

        signal.signal(signal.SIGQUIT, self.die_gracefully)
        signal.signal(signal.SIGINT, self.die_immediately)
        signal.signal(signal.SIGTERM, self.die_immediately)

    def serve_forever(self):
        log.info("Fired up; ready to go!", pid=True)
        while self.alive:
            try:
                socks, _, _ = select.select([self.sock, self.rpipe],
                                            [], [], 5)
            except select.error, e:
                if e[0] == errno.EINTR:
                    continue
                elif e[0] == errno.EBADF:
                    log.error("select() returned EBADF.", pid=True)
                    break
            if self.sock in socks:
                try:
                    connection, address = self.sock.accept()
                    self.handle_request(connection, address)
                    self.requests += 1
                except socket.error, e:
                    self.errors += 1
                    if e[0] == errno.EPIPE:
                        log.error("Broken pipe", pid=True)
                    elif e[0] == errno.EINTR:
                        log.error("accept() interrupted", pid=True)
                finally:
                    try:
                        connection.close()
                    except:
                        pass
        log.info("Worker shutting down", pid=True)
        sys.exit(0)

    def die_gracefully(self, signum, frame):
        self.alive = False
        os.write(self.wpipe, ".")

    def die_immediately(self, signum, frame):
        sys.exit(0)

    def handle_request(self, connection, address):
        start = time.time()
        self.stream = IOStream(connection)
        self.headers_sent = False
        reqline, env = self.parse_request(address)
        status, length = self.execute_request(self.app, env)
        self.stream.close()
        finish = time.time()
        log.request(address[0], reqline, status[:3], length, finish - start)

    def parse_request(self, client_address):
        header_data = self.stream.read_until("\r\n\r\n")
        lines = header_data.split("\r\n")
        reqline = lines[0]
        method, path, protocol = reqline.split(" ", 2)

        env = self.baseenv.copy()
        env["REQUEST_METHOD"] = method
        env["SERVER_PROTOCOL"] = protocol
        env["REMOTE_ADDR"] = client_address[0]
        if "?" in path:
            path, _, query = path.partition("?")
            env["QUERY_STRING"] = query
        env["PATH_INFO"] = urlunquote(path)
        env["wsgi.input"] = self.stream

        for line in lines[1:]:
            if not line: break
            key, _, value = line.partition(":")
            key = key.replace("-", "_").upper()
            value = value.strip()
            env["HTTP_" + key] = value
        return reqline, env

    def execute_request(self, app, env):
        data = []
        response = [None, [], data]
        def start_response(status, response_headers, exc_info=None):
            if exc_info is not None:
                try:
                    if self.headers_sent:
                        raise exc_info[0], exc_info[1], exc_info[2]
                finally:
                    exc_info = None

            response[0:2] = [status, response_headers]
            return data.append
        chunks = self.app(env, start_response)
        status, headers, data = response
        length = self.send_response(status, headers, chunks, data)
        return status, length

    def send_headers(self, status, headers):
        write = self.stream.write
        if not self.headers_sent:
            write("HTTP/1.1 %s\r\n" % status)
            write("Date: %s\r\n" % (http_date(),))
            for header in headers:
                if header[0].lower() not in ("connection", "date"):
                    write("%s: %s\r\n" % header)
            write("Connection: close\r\n")
            write("\r\n")
            self.headers_sent = True
            self.stream.flush()

    def send_response(self, status, headers, chunks, data=None):
        write = self.stream.write
        length = 0
        for chunks in [data, chunks, [""]]:
            for chunk in chunks:
                if not self.headers_sent:
                    self.send_headers(status, headers)
                write(chunk)
                length += len(chunk)
        if hasattr(chunks, "close"):
            chunks.close()
        return length

if __name__ == '__main__':
    hello_world = dict(app=demo_app,
                       address=("127.0.0.1", 8080),
                       worker_processes=4,
                       access_log="access_log",
                       error_log="error_log")
    
    HornedManager(hello_world).serve_forever()



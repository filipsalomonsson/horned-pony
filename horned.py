#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import socket
import select
import signal
import errno
import struct
from cStringIO import StringIO

try:
    import ctypes
    libc = ctypes.cdll.LoadLibrary("libc.so.6")
    get_errno_loc = libc.__errno_location
    get_errno_loc.restype = ctypes.POINTER(ctypes.c_int)

    def errcheck(ret, func, args):
        if ret == -1:
            e = get_errno_loc()[0]
            raise OSError(os.strerror(e))
        return ret

    sendfile = libc.sendfile
    sendfile.errcheck = errcheck
except:
    def sendfile(outfile, infile, offset, length):
        pass

DEBUG, INFO, ERROR = 1, 2, 3
class Logger(object):
    def __init__(self, stream="test.log", level=INFO):
        if isinstance(stream, basestring):
            self.stream = open(stream, "a", 0)
        else:
            self.stream = stream
        self.level = level

    def reopen(self):
        filename = self.stream.name
        if not filename.startswith("<"):
            try:
                stream = open(filename, "a", 0)
            except IOError:
                self.error("Couldn't reopen log file %s; leaving open.",
                           filename)
            else:
                self.stream.close()
                self.stream = stream

    def error(self, msg, *args, **kwargs):
        if self.level <= ERROR:
            self.write("error", msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        if self.level <= INFO:
            self.write("info", msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        if self.level <= DEBUG:
            self.write("debug", msg, *args, **kwargs)

    def write(self, level, msg, *args, **kwargs):
       timestamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
       line = "%s %s" % (timestamp, msg % args)
       if "pid" in kwargs:
           prefix = "(#%d) " % kwargs["pid"]
           line = prefix + line
       self._write(line)

    def _write(self, data):
        self.stream.write(data + "\n")
        self.stream.flush()

logging = Logger()

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
    return ["<html><body><h1>Hello world!</h1></body></html>\n\n"]# + ["%s=%s\n" % item for item in sorted(environ.items())]

def sendfile_app(environ,start_response):
    try:
        file = open(environ.get("PATH_INFO", "")[1:])
        start_response("200 OK", [('Content-Type','text/plain')])
        return file
    except IOError:
        start_response("404 Not Found", [('Content-Type', 'text/plain')])
        return ["Could not open %s." % environ.get("PATH_INFO")]

status_struct = struct.Struct("bqqq")
STARTING = 1
WAITING = 2
PROCESSING = 3
SHUTTING_DOWN = 4
DEAD = 5

STATUS = {
    STARTING: "starting",
    WAITING: "waiting",
    PROCESSING: "processing",
    SHUTTING_DOWN: "shutting down",
    DEAD: "dead",
}

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


class HornedSocket(object):
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


class HornedManager(object):
    def __init__(self, app, worker_processes=4):
        self.app = app
        self.worker_processes = worker_processes
        self.base_environ = {}
        self.workers = set()
        self.alive = True

        signal.signal(signal.SIGINT, self.die_gracefully)
        signal.signal(signal.SIGHUP, self.report_status)

    def listen(self):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', 6666))
        self.sock.listen(1024)

    def serve_forever(self):
        while self.alive:
            self.cleanup_workers()
            self.spawn_workers()
            self.update_status()
            time.sleep(1)
        for worker in self.workers:
            worker.die_gracefully()
        t = time.time()
        while self.workers:
            if time.time() - t > 10:
                logging.error("%d children won't die. Exiting anyway.",
                              len(self.children))
                break
            for worker in list(self.workers):
                pid, status = worker.wait(os.WNOHANG)
                if pid:
                    self.workers.remove(worker)
            time.sleep(0.1)

    def cleanup_workers(self):
        for worker in list(self.workers):
            pid, status = os.waitpid(worker.pid, os.WNOHANG)
            if pid:
                self.workers.remove(worker)
                logging.info("Worker #%d died" % pid)

    def spawn_workers(self):
        while len(self.workers) < self.worker_processes:
            worker = HornedWorker(self.sock, self.app)
            self.workers.add(worker)
            worker.run()

    def update_status(self):
        for worker in self.workers:
            worker.update_status()

    def report_status(self, *args):
        for worker in self.workers:
            print "Worker #%d: %s. %d requests, %d errors" % \
                (worker.pid,
                 STATUS.get(worker.status, "unknown"),
                 worker.requests,
                 worker.errors)

    def die_gracefully(self, signum, frame):
        self.alive = False


class HornedWorker:
    def __init__(self, sock, app):
        self.sock = sock
        self.app = app
        self.pid = None
        self.status = STARTING
        self.timestamp = int(time.time())
        self.requests = self.errors = 0

        r, w = os.pipe()
        self.rpipe = os.fdopen(r, "r", 0)
        self.wpipe = os.fdopen(w, "w", 0)

    def run(self):
        pid = os.fork()
        if pid:
            self.wpipe.close()
            logging.info("Spawned worker #%d" % pid)
            self.pid = pid
        else:
            self.rpipe.close()
            HornedWorkerProcess(self.sock, self.app, self.wpipe).serve_forever()

    def update_status(self):
        while select.select([self.rpipe], [], [], 0)[0]:
            data = self.rpipe.read(status_struct.size)
            data = status_struct.unpack(data)
            (self.status, self.timestamp, self.requests, self.errors) = data

    def die_gracefully(self):
        logging.info("Sending SIGINT to worker #%d" % self.pid)
        os.kill(self.pid, signal.SIGINT)

    def wait(self, *options):
        return os.waitpid(self.pid, *options)


class HornedWorkerProcess(object):
    def __init__(self, sock, app, status_pipe):
        self.sock = sock
        self.app = app
        self.status_pipe = status_pipe
        self.alive = True
        self.requests = 0
        self.errors = 0

        self.rpipe, self.wpipe = os.pipe()

        env = self.baseenv = os.environ.copy()
        host, port = sock.getsockname()[:2]
        env["SERVER_NAME"] = socket.getfqdn(host)
        env["SERVER_PORT"] = str(port)

        signal.signal(signal.SIGINT, self.die_gracefully)

    def serve_forever(self):
        while self.alive:
            self.report_status(WAITING)
            try:
                socks, _, _ = select.select([self.sock, self.rpipe], [], [])
            except select.error, e:
                if e[0] == errno.EINTR:
                    continue
            for sock in socks:
                self.report_status(PROCESSING)
                try:
                    connection, address = sock.accept()
                    self.handle_request(connection, address)
                    self.requests += 1
                except socket.error, e:
                    self.errors += 1
                    if e[0] == errno.EPIPE:
                        logging.error("Broken pipe")
                    elif e[0] == errno.EINTR:
                        logging.error("accept() interrupted")
                finally:
                    connection.close()
        logging.info("Shutting down")
        sys.exit(0)

    def report_status(self, status):
        try:
            self.status_pipe.write(status_struct.pack(status,
                                                      int(time.time()),
                                                      self.requests,
                                                      self.errors))
        except IOError:
            if self.alive:
                logging.error("Parent gone!")
                self.alive = False

    def die_gracefully(self, signum, frame):
        self.alive = False
        self.report_status(SHUTTING_DOWN)
        os.write(self.wpipe, ".")

    def handle_request(self, connection, address):
        start = time.time()
        self.initialize_request(connection, address)
        env = self.parse_request()
        status, length = self.execute_request(self.app, env)
        self.finalize_request(connection, address)
        finish = time.time()
        request = "%s %s %s" % (env["REQUEST_METHOD"],
                                env["PATH_INFO"],
                                env["SERVER_PROTOCOL"])
        logging.info('%s "%s" %s %s %f',
                     address[0], request, status[:3], length, finish - start)

    def initialize_request(self, connection, address):
        self.stream = HornedSocket(connection)
        self.client_address = address
        self.headers_sent = False

    def parse_request(self):
        header_data = self.stream.read_until("\r\n\r\n")
        lines = header_data.split("\r\n")
        reqline = lines[0]
        method, path, protocol = reqline.split(" ", 2)

        env = self.baseenv.copy()

        env["SERVER_PROTOCOL"] = protocol
        env["REQUEST_METHOD"] = method
        env["REMOTE_ADDR"] = self.client_address[0]
        env["SCRIPT_NAME"] = ""
        if "?" in path:
            path, _, query = path.partition("?")
            env["QUERY_STRING"] = query
        env["PATH_INFO"] = urlunquote(path)

        env["wsgi.version"] = (1, 0)
        env["wsgi.url_scheme"] = "http"
        env["wsgi.input"] = self.stream.socket.makefile("rb", -1)
        env["wsgi.errors"] = sys.stderr
        env["wsgi.multithread"] = False
        env["wsgi.multiprocess"] = True
        env["wsgi.run_once"] = False

        for line in lines[1:]:
            if not line:
                break
            key, _, value = line.partition(":")
            key = key.replace("-", "_").upper()
            value = value.strip()
            env["HTTP_" + key] = value

        return env

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
        if isinstance(chunks, file):
            length = self.send_file(status, headers, chunks)
        else:
            length = self.send_response(status, headers, chunks, data)
        return status, length

    def finalize_request(self, connection, address):
        self.stream.close()

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

    def send_file(self, status, headers, file):
        self.send_headers(status, headers)
        offset = ctypes.pointer(ctypes.c_int(0))
        length = ctypes.c_int(os.fstat(file.fileno()).st_size)
        retval = sendfile(self.stream.socket.fileno(),
                          file.fileno(),
                          offset,
                          length)
        file.close()
        return length.value

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
    worker = HornedManager(demo_app)
    worker.listen()
    worker.serve_forever()


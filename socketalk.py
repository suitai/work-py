#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import time
import yaml
import json
import signal
import socket
import daemon
import lockfile
import logging
import threading
import subprocess
import Queue
import SocketServer


SOCKET_BUFFER_SIZE = 4096
POPEN_BUFFER_SIZE = 4096
MAIN_CONDITION_WAIT = 300
QUEUE_CONDITION_WAIT = 30
SOCKET_TIMEOUT = 30
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 50010
CONFIG_FILE = "socketalk.yaml"
PID_FILE = "/var/run/socketalk/socketalk.pid"
QID_FILE = "/var/run/socketalk/socketalk.qid"
WORK_DIR = "/home/socketalk/"


## class
class ThreadedServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


class Listen(object):

    logformat = "%(asctime)-15s socketalk: [%(levelname)s] %(message)s"
    loglevels = {'debug': logging.DEBUG, 'info': logging.INFO,
                 'warn': logging.WARN, 'error': logging.ERROR}
    signames = {1: 'SIGHUP', 2: 'SIGINT', 15: 'SIGTERM'}

    def __init__(self, configfile):
        self.status = "Init"
        self.logger = None
        self.server = None
        self.queue = Queue.Queue()
        self.condition = threading.Condition()
        self.queue_condition = threading.Condition()
        self.receive_work = {'execute': self.execute,
                             'enqueue': self.enqueue,
                             'write': self.write}
        self.dequeue_work = {'execute': self.execute}
        self.signal_work = {'SIGINT': self.stop,
                            'SIGTERM': self.stop}

        self.read_conf(configfile)
        self.set_log_handler()
        self.set_signal_handler()

    def read_conf(self, configfile):
        """ read a configfile """
        with open(configfile, 'r') as f:
            data = yaml.load(f)
        if data:
            self.host = data['host'] if "host" in data else DEFAULT_HOST
            self.port = data['port'] if "port" in data else DEFAULT_PORT
            self.loglevel = data['loglevel'] if "loglevel" in data else "debug"

    def set_log_handler(self):
        """ set a self.log_handler """
        if not self.loglevel:
            assert False, "do not load a config"

        log_handler = logging.StreamHandler()
        log_handler.level = self.loglevels.get(self.loglevel, logging.INFO)
        log_handler.formatter = logging.Formatter(self.logformat)

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(log_handler)

    def signal_handler(self, signum, frame):
        """ handle signal to stop """
        self.logger.info("received signal(%s)", self.signames[signum])
        self.signal_work[self.signames[signum]]()

    def set_signal_handler(self):
        """ set a signal.signal """
        if not self.logger:
            assert False, "do not set a logger"

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def start_server_thread(self):
        """ start a server thread """
        if not self.host and self.port:
            assert False, "do not know a host and a port"

        logger = self.logger
        receive_work = self.receive_work

        class RequestHandler(SocketServer.BaseRequestHandler):
            def handle(self):
                message = self.request.recv(SOCKET_BUFFER_SIZE)
                host, port = self.client_address
                logger.info("get message from %s:%s" % (host, port))
                logger.debug("message \"%s\"" % message)
                data = json.loads(message)
                if "type" in data and data['type'] in receive_work:
                    responce = receive_work[data['type']](data)
                else:
                    responce = "cannot correspond for \"%s\"" % message
                    logger.warn(responce)
                self.request.send(responce)

        self.server = ThreadedServer((self.host, self.port), RequestHandler)
        server_thread = threading.Thread(target=self.server.serve_forever, name="socket_server")
        server_thread.daemon = True
        server_thread.start()
        host, port = self.server.server_address
        self.logger.info("start server @ %s", "%s:%s" % (host, port))

    def start_queue_thread(self):
        """ start a queue_handler thread """
        logger = self.logger
        queue = self.queue
        queue_condition = self.queue_condition
        dequeue_work = self.dequeue_work

        def queue_handler():
            """ handle queue items """
            while True:
                try:
                    item = queue.get()
                except Queue.Empty:
                    with queue_condition:
                        queue_condition.wait(QUEUE_CONDITION_WAIT)
                else:
                    data = json.loads(item)
                    logger.info("dequeue [%d] (%d items left)" % (data['qid'], queue.qsize()))
                    logger.debug("item \"%s\"" % item)
                    if "type" in data and data['type'] in dequeue_work:
                        result = dequeue_work[data['type']](data)
                    else:
                        result = "cannot correspond for \"%s\"" % item
                        logger.warn(result)
                    resultfile = "queue-%s.out" % (str(data['qid']).zfill(3))
                    with open(resultfile, 'w') as f:
                        f.write(result)
                    queue.task_done()

        queue_thread = threading.Thread(target=queue_handler, name="queue_handler")
        queue_thread.daemon = True
        queue_thread.start()
        self.logger.info("start queue")

    def start(self):
        """ start a server """
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))
        if os.path.exists(QID_FILE):
            with open(QID_FILE, 'r') as f:
                self.qid = int(f.read())
        else:
            self.qid = 0

        self.logger.info("start socketalk")
        self.start_queue_thread()
        self.start_server_thread()
        while self.status != "Stop":
            with self.condition:
                self.condition.wait(MAIN_CONDITION_WAIT)
        self.server.shutdown()
        self.logger.info("stop socketalk")

    def start_daemon(self, logfile):
        lock = lockfile.FileLock(PID_FILE)
        if lock.is_locked():
            print lock.path, "is locked."
            return

        with open(logfile, 'a') as f:
            context = daemon.DaemonContext(pidfile=lock, stdout=f, stderr=f,
                                           working_directory=WORK_DIR)
            context.signal_map = {signal.SIGTERM: self.signal_handler,
                                  signal.SIGHUP: 'terminate'}
            with context:
                self.start()

    def stop(self):
        """ stop a server """
        self.status = "Stop"
        with self.condition:
            self.condition.notify_all()

    def enqueue(self, data):
        """ enqueue data """
        if not "item" in data:
            return "cannot find \'item\' key"

        data['item']['qid'] = self.qid
        item = json.dumps(data['item'])
        self.queue.put(item)
        self.logger.debug("enqueue [%d]" % self.qid)
        self.logger.debug("item \"%s\"" % item)
        with self.queue_condition:
            self.queue_condition.notify()
        with open(QID_FILE, 'w') as f:
            f.write(str(self.qid))
        self.qid += 1
        return "enqueue [%d]" % (self.qid - 1)

    def execute(self, data):
        """ command exec """
        if not "command" in data:
            return "cannot find \'command\' key"
        if not "uid" in data:
            return "cannot find \'uid\' key"
        if not "gid" in data:
            return "cannot find \'gid\' key"

        uid = data['uid']
        gid = data['gid']

        def preexec_fn():
            os.setgid(uid)
            os.setuid(gid)

        try:
            pid = subprocess.Popen(data['command'].split(), bufsize=POPEN_BUFFER_SIZE,
                                   cwd=WORK_DIR, preexec_fn=preexec_fn,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = pid.communicate()[0]
        except OSError, detail:
            output = str(detail)

        self.logger.debug("execute \"%s\"" % data['command'])
        return "# %s\n%s" % (data['command'], output.rstrip())

    def write(self, data):
        """ write file """
        if not "message" in data:
            return "cannot find \'message\' key"
        if not "filename" in data:
            return  "cannot find \'filename\' key"

        with open(data['filename'], 'w') as f:
            f.write(data['message'])

        return "success to write \"%s\"" % data['filename']


## funtions
def Speak(message, host, port):
    """ speak through a socket """
    # send message
    speak_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    speak_socket.settimeout(SOCKET_TIMEOUT)
    try:
        speak_socket.connect((host, port))
        speak_socket.send(message)
        return  speak_socket.recv(SOCKET_BUFFER_SIZE)
    except socket.error, detail:
        speak_socket.close()
        return "Error: %s" % detail
    except socket.timeout, detail:
        speak_socket.close()
        return "Error: %s" % detail

def main():
    if not os.path.exists(CONFIG_FILE):
        sys.exit("Error: Can not exists %s" % CONFIG_FILE)

    l = Listen(CONFIG_FILE)
    try:
        l.start()
    except socket.error, detail:
        sys.exit("Error: %s" % detail)


## main
if __name__ == '__main__':
    main()

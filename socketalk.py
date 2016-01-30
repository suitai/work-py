#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import yaml
import time
import signal
import socket
import daemon
import lockfile
import logging
import threading
import subprocess
import Queue
import SocketServer


BUFFER_SIZE = 1024
CONFIG_FILE = "socketalk.yaml"
LOG_FILE = "socketalk.log"
PID_FILE = "/var/run/socketalk/socketalk.pid"
MAIN_CONDITION_WAIT = 300
QUEUE_CONDITION_WAIT = 30
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 50010

## class
class ThreadedServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


class Listen(object):

    def __init__(self, configfile, logfile=None):
        self.status = "Init"
        self.logger = None
        self.server = None
        self.queue = Queue.Queue()
        self.condition = threading.Condition()
        self.queue_condition = threading.Condition()
        self.receive_work = self.enqueue
        self.dequeue_work = self.sleep
        self.signal_work = self.stop

        self.read_conf(configfile)
        self.set_log_handler(logfile)
        self.set_signal_handler()

    def read_conf(self, configfile):
        """ read a configfile """
        with open(configfile, 'r') as f:
            data = yaml.load(f)
        if data:
            self.host = data['host'] if "host" in data else DEFAULT_HOST
            self.port = data['port'] if "port" in data else DEFAULT_PORT
            self.loglevel = data['loglevel'] if "loglevel" in data else "debug"

    def set_log_handler(self, logfile=None):
        """ set a self.log_handler """
        if not self.loglevel:
            assert False, "do not load a config"

        logformat = "%(asctime)-15s socketalk: [%(levelname)s] %(message)s"
        loglevels = {'debug': logging.DEBUG, 'info': logging.INFO,
                     'warn': logging.WARN, 'error': logging.ERROR}

        if logfile:
            log_handler = logging.FileHandler(logfile)
            log_handler.level = loglevels.get(self.loglevel, logging.INFO)
            log_handler.formatter = logging.Formatter(logformat)
        else:
            log_handler = logging.StreamHandler()
            log_handler.level = loglevels.get(self.loglevel, logging.INFO)
            log_handler.formatter = logging.Formatter(logformat)

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(log_handler)

    def signal_handler(self, signum, frame):
        """ handle signal to stop """
        signames = {1: 'SIGHUP', 2: 'SIGINT', 15: 'SIGTERM'}

        self.logger.info("received signal(%s)", signames[signum])
        self.stop()

    def set_signal_handler(self):
        """ set a signal.signal """
        if not self.logger:
            assert False, "do not set a logger"

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

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
                    message = self.queue.get()
                    logger.debug("dequeue mesaage")
                    dequeue_work(message)
                    queue.task_done()
                except Queue.Empty:
                    with queue_condition:
                        queue_condition.wait(QUEUE_CONDITION_WAIT)

        queue_thread = threading.Thread(target=queue_handler, name="queue_handler")
        queue_thread.daemon = True
        queue_thread.start()
        self.logger.debug("start queue")

    def start_server_thread(self):
        """ start a server thread """
        if not self.host and self.port:
            assert False, "do not know a host and a port"

        logger = self.logger
        receive_work = self.receive_work

        class RequestHandler(SocketServer.BaseRequestHandler):
            def handle(self):
                message = self.request.recv(BUFFER_SIZE)
                host, port = self.client_address
                logger.debug("get message from %s:%s" % (host, port))
                responce = receive_work(message)
                self.request.send(responce)

        self.server = ThreadedServer((self.host, self.port), RequestHandler)
        server_thread = threading.Thread(target=self.server.serve_forever, name="socket_server")
        server_thread.daemon = True
        server_thread.start()
        host, port = self.server.server_address
        self.logger.info("start server @ %s", "%s:%s" % (host, port))

    def start(self, pidfile=None):
        """ start a server """
        if pidfile:
            with open(pidfile, 'w') as f:
                f.write(str(os.getpid()))
        self.logger.info("start socketalk")
        self.start_queue_thread()
        self.start_server_thread()
        self.status = "Start"
        while self.status != "Stop":
            with self.condition:
                self.condition.wait(MAIN_CONDITION_WAIT)
        self.server.shutdown()

    def start_daemon(self, pidfile, logfile):
        lock = lockfile.FileLock(pidfile)
        if lock.is_locked():
            print lock.path, "is locked."
            return

        with open(logfile, 'a') as f:
            context = daemon.DaemonContext(pidfile=lock, stdout=f, stderr=f)
            context.signal_map = {signal.SIGTERM: self.signal_handler,
                                  signal.SIGHUP: 'terminate'}
            with context:
                self.start(pidfile)

    def stop(self):
        """ stop a server """
        self.status = "Stop"
        with self.condition:
            self.condition.notify_all()
        self.logger.info("stop socketalk")

    def echo(self, message):
        return message

    def enqueue(self, message):
        self.queue.put(message)
        with self.queue_condition:
            self.queue_condition.notify()
        self.logger.debug("enqueue message")
        return message

    def sleep(self, message):
        self.logger.info("sleep 3")
        time.sleep(3)
        return message


def Speak(message, host, port):
    """ speak through a socket """
    # send message
    speak_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    speak_socket.settimeout(5)
    try:
        speak_socket.connect((host, port))
        speak_socket.send(message)
    except socket.error, detail:
        speak_socket.close()
        return "Error: %s" % detail, 1
    except socket.timeout, detail:
        speak_socket.close()
        return "Error: %s" % detail, 1
    else:
        # receive message
        message = speak_socket.recv(BUFFER_SIZE)
        return message, 1

## funtions
def listen():
    if not os.path.exists(CONFIG_FILE):
        sys.exit("Error: Can not exists %s" % CONFIG_FILE)

    l = Listen(CONFIG_FILE)
    try:
        l.start()
    except socket.error, detail:
        sys.exit("Error: %s" % detail)

def listend():
    if not os.path.exists(CONFIG_FILE):
        sys.exit("Error: Can not exists %s" % CONFIG_FILE)

    l = Listen(CONFIG_FILE)
    try:
        l.start_daemon(PID_FILE, LOG_FILE)
    except socket.error, detail:
        sys.exit("Error: %s" % detail)

def kill_listend():
    if not os.path.exists(PID_FILE):
        sys.exit("not running")

    with open(PID_FILE, 'r') as file:
        pid = int(file.read())
    try:
        os.kill(pid, signal.SIGTERM)
        os.remove(PID_FILE)
    except OSError, detail:
        sys.exit("Error: %s" % detail)

def status_listend():
    if not os.path.exists(PID_FILE):
        sys.exit("not running")

    with open(PID_FILE, 'r') as file:
        pid = file.read()
    try:
        open(os.path.join("/proc", pid, "cmdline"), 'rb')
        print "running"
    except IOError:
        sys.exit("not running")

def speak(message):
    if not os.path.exists(CONFIG_FILE):
        sys.exit("Error: Can not exists %s" % CONFIG_FILE)

    with open(CONFIG_FILE, 'r') as f:
        data = yaml.load(f)
    if "host" in data and "port" in data:
        host = data['host']
        port = data['port']
        response, result = Speak(message, host, port)
        if result != 0:
            sys.exit(response)
        else:
            print response

def less_log():
    if not os.path.exists(LOG_FILE):
        sys.exit("Error: Can not exists %s" % LOG_FILE)

    cmd = ['/usr/bin/less', LOG_FILE]
    pid = subprocess.Popen(cmd)

    def send_signal(signum, frame):
        pid.send_signal(signal.SIGINT)

    signal.signal(signal.SIGINT, send_signal)
    pid.communicate()

## main
def main():
    listen()


if __name__ == '__main__':
    main()

#!/usr/bin/env python
""" talk through a socket.socket """

import os
import sys
import socket
from threading import Thread, Condition
from Queue import Queue, Empty

from daemon import DaemonContext
from lockfile.pidlockfile import PIDLockFile

HOST = '127.0.0.1'
PORT = 50009
RECV_SIZE = 1024
SOCK_WAIT = 100
COND_WAIT = 10
QUEUE_WAIT = 0.1

class Listener:
    """ listen through a socket.socket """
    def __init__(self, host=HOST, port=PORT):
        """ prepare """
        self.host = host
        self.port = port

    def daemonize(self, filename='/var/run/talk.pid'):
        """ deamone process """
        fileout = open('talk.log', 'w+')
        filelock = PIDLockFile(filename)
        # check lockfile
        if filelock.is_locked():
            message = '%s already exists, exitting\n' % filelock.lock_file
            sys.exit(message)

        context = DaemonContext(pidfile=filelock,
                                stdout=fileout, stderr=fileout)
        with context:
            self.listen()

    def listen(self):
        """  listen the self.socket continuously """
        sys.stdout.write('-- listen start --\n')
        self.queue = Queue()
        self.condition = Condition()
        # start a dequeue process and wait start
        with self.condition:
            self.dequeue_thread = Thread(target=self._dequeue)
            self.dequeue_thread.setDaemon(True)
            self.dequeue_thread.start()
            self.condition.wait()
        # socket bind
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(SOCK_WAIT) 
        try:
            self.socket.bind((self.host, self.port))
        except socket.error:
            sys.stdout.write('talk: socket error\n')
            sys.exit()
        sys.stdout.write('talk: listen @ %s:%s\n' % (self.host, self.port))
        # listen continuously
        while self.dequeue_thread.is_alive():
            self.socket.listen(1)
            try:
                conn, addr = self.socket.accept()
                # wait to be connected
                sys.stdout.write('talk: connected by %s: %d\n' % addr)
            except socket.timeout:
                continue
            except KeyboardInterrupt:
                break
            # enqueue on another thread
            enqueue_thread = Thread(target=self._enqueue, args=(conn, addr))
            enqueue_thread.setDaemon(True)
            enqueue_thread.start()
        else:
            sys.stdout.write('talk: dequeue process is dead\n')
        # end
        self.socket.close()
        sys.stdout.write('-- listen end --\n')

    def _enqueue(self, conn, addr):
        """ enqueue to the self.queue """
        message_list = []
        # receive a message
        message = conn.recv(RECV_SIZE)
        sys.stdout.write('talk: enqueue "%s"\n' % message)
        conn.close()
        # enqueue
        with self.condition:
            self.queue.put(message)
            self.condition.notify()

    def _dequeue(self):
        """ dequeue from the self.queue """
        sys.stdout.write('talk: start a dequeue process\n')
        # notify that this process start
        with self.condition:
            self.condition.notify()
        # dequeu continuously
        while True:
            with self.condition:
                try:
                    message = self.queue.get(timeout=QUEUE_WAIT)
                    sys.stdout.write('talk: dequeue "%s"\n' % message)
                    self._react(message)
                except Empty:
                    try:
                        self.condition.wait(COND_WAIT)
                        # wait to be enqueued
                    except:
                        break

    def _react(self, message):
        """ react by a message """
        pass

def speak(message, host=HOST, port=PORT):
    """ speak through a socket.socket """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.send(message)
    sock.close()

if __name__ == "__main__":
    L = Listener()
    L.daemonize()

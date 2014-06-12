#!/usr/bin/env python
""" talk through a socket.socket """

import os
import sys
import socket
from getopt import getopt, GetoptError
from signal import SIGTERM
from threading import Thread, Condition
from Queue import Queue, Empty
from daemon import DaemonContext
from lockfile.pidlockfile import PIDLockFile

HOST = '127.0.0.1'
PORT = 50010
PID_FILE = '/var/run/talk.pid'
LOG_FILE = 'talk.log'
RECV_SIZE = 1024
SOCK_WAIT = 100
COND_WAIT = 10
QUEUE_WAIT = 0.1

class Listener:
    """ listen through a socket.socket """
    def __init__(self, host=HOST, port=PORT, pidfile=PID_FILE, logfile=LOG_FILE):
        """ prepare """
        self.host = HOST
        self.port = port
        self.pidfile = pidfile
        self.logfile = logfile
        self.queue = Queue()
        self.condition = Condition()

    def daemonize(self):
        """ deamone process """
        try:
            fileout = open(self.logfile, 'a')
        except IOError, detail:
            sys.exit('IOError: %s' % detail)
        filelock = PIDLockFile(self.pidfile)
        # check lockfile
        if filelock.is_locked():
            sys.exit('%s already exists, exitting' % filelock.lock_file)
        # daemonize
        context = DaemonContext(pidfile=filelock, stdout=fileout, stderr=fileout)
        sys.stdout.write('talk listen start @ %s:%d\n' %  (self.host, self.port))
        with context:
            self.main()

    def kill(self):
        """ kill the daemon process"""
        try:
            pidfile = open(self.pidfile, 'r')
        except IOError, detail:
            sys.exit('IOError: %s' % detail)
        pid = int(pidfile.read())
        os.kill(pid, SIGTERM)

    def main(self):
        self.start_react()
        self.start_listen(host=self.host, port=self.port)

    def start_react(self):
        """ start a dequeue thread """
        self.dequeue_thread = Thread(target=self._dequeue)
        self.dequeue_thread.setDaemon(True)
        self.dequeue_thread.start()

    def start_listen(self, host, port):
        """ start to listen the self.socket continuously """
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(SOCK_WAIT) 
        try:
            self.socket.bind((host, port))
            sys.stdout.write('talk: listen @ %s:%d\n' % (host, port))
        except socket.error, detail:
            sys.exit('socket error%s' % detail)
        while True:
            # listen continuously
            self.socket.listen(1)
            try:
                conn, addr = self.socket.accept()
                # wait to be connected
                sys.stdout.write('talk: connected by %s:%d\n' % addr)
            except socket.timeout:
                continue
            except KeyboardInterrupt:
                break
            except Exception:
                break
            # enqueue on another thread
            enqueue_thread = Thread(target=self._enqueue, args=(conn, addr))
            enqueue_thread.setDaemon(True)
            enqueue_thread.start()
        # end
        self.socket.close()
        sys.stdout.write('talk: listen end\n')

    def _enqueue(self, conn, addr):
        """ enqueue to the self.queue """
        message_list = []
        # receive a message
        message = conn.recv(RECV_SIZE)
        sys.stdout.write('talk: enqueue "%s"\n' % message)
        conn.close()
        # enqueue check and enqueue
        do_enqueue = self.pre_enqueue(message)
        if do_enqueue:
            with self.condition:
                self.queue.put(message)
                self.condition.notify()

    def pre_enqueue(self, message):
        """ before enque the message """
        return True
        
    def _dequeue(self):
        """ dequeue from the self.queue """
        sys.stdout.write('talk: start a dequeue thread\n')
        # dequeu continuously
        while True:
            with self.condition:
                try:
                    message = self.queue.get(timeout=QUEUE_WAIT)
                    sys.stdout.write('talk: dequeue "%s"\n' % message)
                    self.react(message)
                except Empty:
                    self.condition.wait(COND_WAIT)

    def react(self, message):
        """ react the message """
        pass

def speak(message, host=HOST, port=PORT):
    """ speak through a socket.socket """
    # send message
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        sock.send(message)
    except socket.error, detail:
        sys.exit('socket error "%s"' % detail)

if __name__ == "__main__":
    """ command execution """
    action = ''
    opt_port = PORT
    opt_host = HOST
    opt_kill = False
    opt_daemonize = False 
    arg_list = sys.argv[1:]
    try:
        action = arg_list.pop(0)
    except IndexError:
        pass
    option = 'p:h:'
    long_option = ['daemonize', 'kill']
    # handle options
    try:
        opt_list, odd_arg_list = getopt(arg_list, option, long_option)
    except GetoptError, detail:
        sys.exit('GetoptError: %s' % detail)
    for opt, arg in opt_list:
        if opt == '-p':
            opt_port = arg
        if opt == '-h':
            opt_host = arg 
        if opt == '--daemonize':
            opt_daemonize = True
        if opt == '--kill':
            opt_kill = True 
    if action == 'listen':
        l = Listener(host=opt_host, port=opt_port)
        # kill the listen daemon
        if opt_kill:
            l.kill()
            sys.exit()
        # start listen
        if opt_daemonize:
            l.daemonize()
        else:
            l.main()
    elif action == 'speak':
        # speak
        message = odd_arg_list[0]
        speak(message, host=opt_host, port=opt_port)

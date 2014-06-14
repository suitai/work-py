#!/usr/bin/env python
""" talk through a socket.socket """

import os
import sys
import socket
from getopt import getopt, GetoptError
from threading import Thread, Condition
from Queue import Queue, Empty

SELF_HOST = '127.0.0.1:50010'
RECV_SIZE = 1024
SOCK_WAIT = 100
COND_WAIT = 10
QUEUE_WAIT = 0.1
THREAD_WAIT = 10

class Listener:
    """ listen through a socket.socket """
    def __init__(self, more_host=None):
        """ prepare """
        self.queue = Queue()
        self.condition = Condition()
        self.host_list = [SELF_HOST,]
        self.manage_list = [self._manage,]
        self.listen_list = []
        if more_host:
            self.host_list.append(more_host)
            self.manage_list.append(None)

    def main(self):
        """ main function """
        self.dequeue_thread = self._thread(self._dequeue)
        for host, manage in zip(self.host_list, self.manage_list):
            addr, port = _get_addr(host)
            listen_thread = self._thread(self._listen, args=(addr,port,manage))
            self.listen_list.append(listen_thread)
        # wait thread
        while True:
            try:
                self.dequeue_thread.join(THREAD_WAIT)
            except KeyboardInterrupt:
                break
            except Exception:
                break

    def _thread(self, target, args=None):
        """ start a thread """
        if args:
            thre = Thread(target=target, args=args)
        else:
            thre = Thread(target=target)
        thre.setDaemon(True)
        thre.start()
        return thre

    def _listen(self, host, port, function=None):
        """ start to listen the self.socket continuously """
        # socket bind
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(SOCK_WAIT) 
        try:
            self.socket.bind((host, port))
            sys.stdout.write('talk: listen @ %s:%d\n' % (host, port))
        except socket.error, detail:
            sys.exit('socket error%s' % detail)
        # listen continuously
        while True:
            self.socket.listen(1)
            try:
                conn, addr = self.socket.accept()
                # wait to be connected
                sys.stdout.write('talk: connected by %s:%d\n' % addr)
            except socket.timeout:
                continue
            # enqueue on another thread
            enqueue_thread = self._thread(self._enqueue, args=(conn,addr,function))
        self.socket.close()
        sys.stdout.write('talk: listen end\n')

    def _enqueue(self, conn, addr, function):
        """ enqueue to the self.queue """
        do_enqueue = True
        message_list = []
        # receive a message
        message = conn.recv(RECV_SIZE)
        conn.close()
        # enqueue check and enqueue
        if function:
            do_enqueue = function(addr, message)
        if do_enqueue:
            with self.condition:
                self.queue.put(message)
                self.condition.notify()
            sys.stdout.write('talk: enqueue "%s"\n' % message)

    def _manage(self, addr, message):
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

    def add_host(self, host, func):
        pass

def speak(message_list, host=SELF_HOST):
    """ speak through a socket.socket """
    # send message
    addr, port = _get_addr(host)
    message = ' '.join(message_list)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((addr, port))
        sock.send(message)
    except socket.error, detail:
        sys.exit('socket error "%s"' % detail)

def _get_addr(host):
    host_split = host.split(':')
    host_addr = host_split[0]
    host_port = int(host_split[1])
    return host_addr, host_port

if __name__ == "__main__":
    pass

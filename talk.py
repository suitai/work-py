#!/usr/bin/env python
""" talk through a socket.socket """

import os
import sys
import socket
from threading import Thread, Condition, currentThread
from Queue import Queue, Empty

SELF_HOST = '127.0.0.1'
SELF_PORT = '50010'
SELF_ADDR = SELF_HOST + ':' + SELF_PORT
RECV_SIZE = 1024
SOCK_WAIT = 100
COND_WAIT = 10
QUEUE_WAIT = 0.1
THREAD_WAIT = 10

class Listener:
    """ listen through a socket.socket """
    def __init__(self, more_addr=None):
        """ prepare """
        self.queue = Queue()
        self.queue_condition = Condition()
        self.listen_condition = Condition()
        self.addr_list = [SELF_ADDR,]
        self.manage_list = [self._manage,]
        if more_addr:
            addr_list = more_addr.split(',')
            for addr in addr_list:
                self.addr_list.append(addr)
                self.manage_list.append(None)

    def main(self):
        """ main function """
        self.listen_list = {}
        self.dequeue_thread = self._thread(self._dequeue)
        for addr, manage in zip(self.addr_list, self.manage_list):
            self.add_socket(addr, manage)
        # join thread
        while True:
            try:
                self.dequeue_thread.join(THREAD_WAIT)
            except KeyboardInterrupt:
                break
            except Exception:
                break

    def add_socket(self, addr, manage=None):
        """ add a socket """
        host, port = _split_addr(addr)
        listen_thread = self._thread(self._listen, args=(host,port))
        ident = listen_thread.ident
        listen_info = {'addr':addr, 'thread':listen_thread, 'manage':manage}
        with self.listen_condition:
            self.listen_list[str(ident)] = listen_info

    def _thread(self, target, args=()):
        """ start a thread """
        thre = Thread(target=target, args=args)
        thre.setDaemon(True)
        thre.start()
        return thre

    def _listen(self, host, port, function=None):
        """ start to listen the self.socket continuously """
        # socket bind
        ident = currentThread().ident
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(SOCK_WAIT)
        try:
            self.socket.bind((host, port))
        except socket.error, detail:
            sys.exit('socket error%s' % detail)
        # listen continuously
        sys.stdout.write('talk: listen @ %s:%d\n' % (host, port))
        while True:
            self.socket.listen(1)
            try:
                conn, addr = self.socket.accept()
                # wait to be connected
                sys.stdout.write('talk: connected by %s:%d\n' % addr)
            except socket.timeout:
                continue
            # enqueue on another thread                       
            enqueue_thread = self._thread(self._enqueue, args=(conn,addr,ident))
        self.socket.close()
        sys.stdout.write('talk: listen end\n')

    def _enqueue(self, conn, addr, ident):
        """ enqueue to the self.queue """
        do_enqueue = True
        message_list = []
        # receive a message
        message = conn.recv(RECV_SIZE)
        conn.close()
        # manage before enqueue
        with self.listen_condition:
            if self.listen_list[str(ident)]['manage']:
                manage = self.listen_list[str(ident)]['manage']
                do_enqueue = manage(addr, message)
        # enqueue
        if do_enqueue:
            with self.queue_condition:
                self.queue.put(message)
                self.queue_condition.notify()
            sys.stdout.write('talk: enqueue "%s"\n' % message)

    def _manage(self, addr, message):
        """ manage the Listener """
        message_list = message.split()
        if addr[0] == SELF_HOST and message_list[0] == 'manage':
            sys.stdout.write('talk: manage "%s"\n' % message)
            if message_list[1] == 'add':
                 if message_list[2] == 'socket':
                     self.add_socket(message_list[3])
                     return False
        return True
        
    def _dequeue(self):
        """ dequeue from the self.queue """
        sys.stdout.write('talk: start a dequeue thread\n')
        # dequeu continuously
        while True:
            with self.queue_condition:
                try:
                    message = self.queue.get(timeout=QUEUE_WAIT)
                    sys.stdout.write('talk: dequeue "%s"\n' % message)
                    self.react(message)
                except Empty:
                    self.queue_condition.wait(COND_WAIT)

    def react(self, message):
        """ react the message """
        pass

def speak(message_list, addr=SELF_ADDR):
    """ speak through a socket.socket """
    # send message
    host, port = _split_addr(addr)
    message = ' '.join(message_list)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        sock.send(message)
    except socket.error, detail:
        sys.exit('socket error "%s"' % detail)

def _split_addr(addr):
    """ get host and port from addr """
    split_addr = addr.split(':')
    host = split_addr[0]
    port = int(split_addr[1])
    return host, port

if __name__ == "__main__":
    pass

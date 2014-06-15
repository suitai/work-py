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
SOCK_WAIT = 10
COND_WAIT = 10
QUEUE_WAIT = 0.1
THREAD_WAIT = 10

class Listener:
    """ listen through a socket.socket """
    def __init__(self, more_addr=None):
        """ prepare """
        self.queue = Queue()
        self.queue_condition = Condition()
        self.addr_list = [SELF_ADDR,]
        self.manage_list = [self.manage,]
        self.listen_info = ThreadInfo()
        if more_addr:
            addr_list = more_addr.split(',')
            for addr in addr_list:
                self.addr_list.append(addr)
                self.manage_list.append(None)

    def main(self):
        """ main function """
        self.dequeue_thread = _add_thread(self._dequeue)
        for addr, manage in zip(self.addr_list, self.manage_list):
            listen_thread = _add_thread(self.add_listen, args=(addr,manage))
        # join thread
        while True:
            try:
             self.dequeue_thread.join(THREAD_WAIT)
            except KeyboardInterrupt, detail:
                sys.stdout.write('talk: %s' % detail)
                break

    def add_listen(self, addr, manage=None):
        """ start to listen the self.socket continuously """
        # add socket and info
        listen_socket = _add_socket(addr)
        listen_ident = currentThread().ident
        listen_info = {'addr':addr, 'thread':currentThread, 'socket':listen_socket, 'manage':manage}
        self.listen_info.set_info(listen_info, ident=listen_ident)
        # listen continuously
        sys.stdout.write('talk: listen start @ %s\n' % addr)
        while True:
            listen_socket.listen(1)
            try:
                conn, addr = listen_socket.accept()
                # wait to be connected
                sys.stdout.write('talk: connected by %s:%d\n' % addr)
            except socket.timeout:
                continue
            # enqueue on another thread                       
            enqueue_thread = _add_thread(self._enqueue, args=(conn,addr,listen_ident))
        listen_socket.close()
        sys.stdout.write('talk: listen end\n')

    def _enqueue(self, conn, addr, ident):
        """ enqueue to the self.queue """
        do_enqueue = True
        # receive a message
        while True: 
            try:
                message = conn.recv(RECV_SIZE)
                break
            except socket.error, detail:
                sys.stderr.write('error "%s", retry' % detail)
                continue
        conn.close()
        # manage before enqueue
        manage = self.listen_info.get_info('manage', ident=ident)
        if manage:
            do_enqueue = manage(addr, message)
        # enqueue
        if do_enqueue:
            with self.queue_condition:
                self.queue.put(message)
                self.queue_condition.notify()
            sys.stdout.write('talk: enqueue "%s"\n' % message)
       
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

    def manage(self, addr, message):
        """ manage the Listener """
        message_list = message.split()
        if addr[0] == SELF_HOST and message_list[0] == 'manage':
            sys.stdout.write('talk: manage "%s"\n' % message)
            if message_list[1] == 'add':
                if message_list[2] == 'listen':
                    self.add_listen(message_list[3])
                    return False
            elif message_list[1] == 'del':
                if message_list[2] == 'listen':
                    self.del_socket(message_list[3])
                    return False
        return True

    def react(self, message):
        """ react the message """
        pass

class ThreadInfo():
    """ info dictionary for threads """
    def __init__(self):
        self.info_condition = Condition()
        self.info_dict = {}

    def get_info(self, key, ident=None):
        """ get listen info """
        if not ident:
            ident = currentThread().ident
        with self.info_condition:
            if self.info_dict[str(ident)][str(key)]:
                return self.info_dict[str(ident)][str(key)]
        return None

    def set_info(self, info, ident=None):
        """ set listen info """
        if not ident:
            ident = currentThread().ident
        with self.info_condition:
            self.info_dict[str(ident)] = info

def speak(message_list, addr=SELF_ADDR):
    """ speak through a socket """
    # send message
    host, port = _split_addr(addr)
    message = ' '.join(message_list)
    speak_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        speak_socket.connect((host, port))
        speak_socket.send(message)
    except socket.error, detail:
        sys.exit('socket error "%s"' % detail)

def _add_thread(target, args=()):
    """ start a thread """
    new_thread = Thread(target=target, args=args)
    new_thread.setDaemon(True)
    new_thread.start()
    return new_thread

def _add_socket(addr):
    """ add a socket from addr """
    host, port = _split_addr(addr)
    # socket bind
    target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target_socket.settimeout(SOCK_WAIT)
    try:
        target_socket.bind((host, port))
    except socket.error, detail:
        sys.exit('socket error%s' % detail)
    return target_socket

def _split_addr(addr):
    """ get host and port from addr """
    port = None
    split_addr = addr.split(':')
    host = split_addr[0]
    if len(split_addr) > 1:
        port = int(split_addr[1])
    return host, port

if __name__ == "__main__":
    pass

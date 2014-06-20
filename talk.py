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
    def __init__(self):
        """ prepare """
        self.queue = Queue()
        self.queue_condition = Condition()
        self.data = ThreadData()
        self.addr_list = [SELF_ADDR,]
        self.manage_list = [self.manage,]

    def main(self):
        """ main function """
        dequeue_thread = _add_thread(self._dequeue)
        for addr, manage in zip(self.addr_list, self.manage_list):
            self.start_listen(addr, manage)
        # join thread
        while True:
            try:
                dequeue_thread.join(THREAD_WAIT)
            except KeyboardInterrupt:
                break
            except Exception, detail:
                sys.stdout.write('talk: %s' % detail)
                break

    def set_addr(self, addr, manage=None):
        self.addr_list.append(addr)
        self.manage_list.append(manage)

    def start_listen(self, addr, manage=None):
        """ start a listen thread """
        listen_thread = _add_thread(self._listen, args=(addr,manage))

    def stop_listen(self, addr):
        """ stop a listen thread with closing the socket """
        listen_ident = self.data.search_info('addr', addr)
        if not listen_ident:
            return
        listen_socket = self.data.get_info('socket',ident=listen_ident)
        listen_socket.close()

    def _listen(self, addr, manage=None):
        """ start to listen the self.socket continuously """
        # add socket and info
        listen_socket = _add_socket(addr)
        listen_info = {'thread':currentThread, 'addr':addr, 'socket':listen_socket, 'manage':manage}
        self.data.set_info(listen_info)
        # listen continuously
        sys.stdout.write('talk: listen start @ %s\n' % addr)
        while True:
            try:
                listen_socket.listen(1)
                conn, addr = listen_socket.accept()
                # wait to be connected
                sys.stdout.write('talk: connected by %s:%d\n' % addr)
            except socket.timeout:
                continue
            except socket.error:
                break
            # enqueue on another thread                       
            enqueue_thread = _add_thread(self._enqueue, args=(conn,addr,manage))
        sys.stdout.write('talk: listen end @ %s\n' % addr)

    def _enqueue(self, conn, addr, manage):
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
                    self.start_listen(message_list[3])
                    return False
            elif message_list[1] == 'del':
                if message_list[2] == 'listen':
                    self.stop_listen(message_list[3])
                    return False
        return True

    def react(self, message):
        """ react the message """
        pass

class ThreadData():
    """ info dictionary for threads """
    def __init__(self):
        self.data_condition = Condition()
        self.data_dict = {}

    def get_info(self, key, ident=None):
        """ get listen info """
        if not ident:
            ident = currentThread().ident
        with self.data_condition:
            if self.data_dict[str(ident)][str(key)]:
                return self.data_dict[str(ident)][str(key)]
        return None

    def set_info(self, info, ident=None):
        """ set listen info """
        if not ident:
            ident = currentThread().ident
        with self.data_condition:
            self.data_dict[str(ident)] = info

    def search_info(self, key, value):
        """ search info return ident """
        with self.data_condition:
            for ident in self.data_dict:
                if self.data_dict[str(ident)][str(key)] == value:
                    return  ident
        return None

def speak(message_list, addr=SELF_ADDR):
    """ speak through a socket """
    # send message
    host, port = _split_addr(addr)
    if not port:
        sys.exit('invalid port number')
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
        target_socket.bind((host, int(port)))
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

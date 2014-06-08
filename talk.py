#!/usr/bin/env python
""" talk through a socket """

import socket
from sys import stdout
from threading import Thread
from multiprocessing import Process, Queue, Condition
from Queue import Empty

HOST = '127.0.0.1'
PORT = 50009

class Listener:
    """ listen through a socket """
    def __init__(self, host=HOST, port=PORT):
        stdout.write('-- start talk.Listener %s : %s --\n' % (host, port))
        # prepare socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((host, port))
        # start a dequeue process
        self.queue = Queue()
        self.condition = Condition()
        self.dequeue_process = Process(target=self._dequeue, args=(self.queue, self.condition))
        self.dequeue_process.start()

        # listen the self.socket continuously
        while True:
            self.socket.listen(1)
            try:
                conn, addr = self.socket.accept()
                # wait to be connected
                stdout.write('talk: connected by %s: %d\n' % addr)
            except KeyboardInterrupt:
                self.dequeue_process.terminate()
                break

            # enqueue on a thread
            enqueue_thread = Thread(target=self._enqueue, args=(conn, addr))
            enqueue_thread.setDaemon(True)
            enqueue_thread.start()

        stdout.write('-- end talk.Listener --\n')

    def _enqueue(self, conn, addr):
        """ enqueue to the self.deque """
        message_list = []

        # receive a message
        while True:
            message = conn.recv(1024)
            # wait for message
            if not message:
                break
            message_list.append(message)
        conn.close()

        message = ''.join(message_list)
        stdout.write('talk: %s\n' % message)

        # enqueue
        with self.condition:
            self.queue.put(message)
            self.condition.notify()

    def _dequeue(self, queue, condition):
        """ dequeue from the self.deque """
        stdout.write('talk: start a dequeue process\n')
        self.queue = queue
        self.condition = condition

        while True:
            with self.condition:
                try:
                    # dequeue
                    message = self.queue.get(timeout=0.1)
                    self.react(message)
                except Empty:
                    self.condition.wait(99999)
                    # wait to be enqueued

    def react(self, message):
        """ react by a message"""
        print message

def speak(message, host=HOST, port=PORT):
    """ speak through a socket"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.send(message)
    sock.close()

if __name__ == "__main__":
    L = Listener()

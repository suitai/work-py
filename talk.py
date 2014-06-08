#!/usr/bin/env python
""" talk through a socket """

import socket
from sys import stdout
from threading import Thread
from multiprocessing import Process, Queue, Condition
from Queue import Empty

HOST = '127.0.0.1'
PORT = 50009
RECV_SIZE = 1024
COND_WAIT = 9999

class Listener:
    """ listen through a socket """
    def __init__(self, host=HOST, port=PORT):
        """ prepare """
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.queue = Queue()
        self.condition = Condition()

    def listen(self, host=HOST, port=PORT):
        """  listen the self.socket continuously """
        stdout.write('-- listen start --\n')
        # start a dequeue process
        self.dequeue_process = Process(target=self._dequeue, args=(self.queue, self.condition))
        self.dequeue_process.start()
        # listen continuously
        self.socket.bind((host, port))
        stdout.write('talk: listen @ %s : %s --\n' % (host, port))
        while self.socket:
            self.socket.listen(1)
            try:
                conn, addr = self.socket.accept()
                # wait to be connected
                stdout.write('talk: connected by %s: %d\n' % addr)
            except KeyboardInterrupt:
                break
            # enqueue on a thread
            enqueue_thread = Thread(target=self._enqueue, args=(conn, addr))
            enqueue_thread.setDaemon(True)
            enqueue_thread.start()
        # terminate & end
        self.queue.close()
        self.socket.close()
        self.dequeue_process.terminate()
        stdout.write('-- listen end --\n')

    def _enqueue(self, conn, addr):
        """ enqueue to the self.queue """
        message_list = []
        # receive a message
        message = conn.recv(RECV_SIZE)
        stdout.write('talk: enqueue "%s"\n' % message)
        conn.close()
        # enqueue
        with self.condition:
            self.queue.put(message)
            self.condition.notify()

    def _dequeue(self, queue, condition):
        """ dequeue from the self.deque """
        stdout.write('talk: start a dequeue process\n')
        # dequeu continuously
        while queue:
            with condition:
                try:
                    message = queue.get(timeout=0.1)
                    stdout.write('talk: dequeue "%s"\n' % message)
                    self.react(message)
                except Empty:
                    condition.wait(COND_WAIT)
                    # wait to be enqueued

    def react(self, message):
        """ react by a message """
        pass

def speak(message, host=HOST, port=PORT):
    """ speak through a socket """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.send(message)
    sock.close()

if __name__ == "__main__":
    L = Listener()
    L.listen()

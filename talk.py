""" talk through a socket.socket """

import os
import sys
import socket
import signal
import logging
from Queue import Queue,Empty
from threading import Thread,Condition,currentThread,active_count
try:
    from daemon import DaemonContext
    from lockfile.pidlockfile import PIDLockFile
except ImportError, detail:
    sys.exit('ImportError: %s' % detail)

LOG_FORMAT = "%(asctime)-15s %(levelname)s %(name)-8s %(message)s"
SELF_HOST = '127.0.0.1'
SELF_PORT = 50010
SELF_ADDR = ('%s:%s' % (SELF_HOST,SELF_PORT))
RECV_SIZE = 4096
QUEUE_WAIT = 0.1
SOCK_WAIT = 5
COND_WAIT = 10
THREAD_WAIT = 100
SOCK_BACKLOG = 1

class Listener:
    """ listen through a socket """
    def __init__(self, logfile=None):
        """ prepare """
        self.condition = Condition()
        self.queue = Queue()
        self.queue_condition = Condition()
        self.data = ListenData()
        self.other_addr_list = []
        # make log
        self.logger = logging.getLogger('Listener')
        self.logger.setLevel(logging.INFO)
        self.handler = None
        if logfile:
            self.handler = logging.FileHandler(logfile)
            self.handler.level = logging.INFO
            self.handler.formatter = logging.Formatter(fmt=LOG_FORMAT)
            self.logger.addHandler(self.handler)
        else:
            logging.basicConfig(format=LOG_FORMAT)

    def add_addr(self, addr):
        """ set an addr """
        self.other_addr_list.append(addr)

    def daemonize(self, pidfile):
        """ deamone process """
        # check lockfile
        filelock = PIDLockFile(pidfile)
        if filelock.is_locked():
            sys.exit('%s already exists, exitting' % filelock.lock_file)
        # daemonize
        context = DaemonContext(pidfile=filelock, files_preserve=[self.handler.stream])
        with context:
            self.start()

    def start(self):
        """ start threads """
        _add_thread(self._dequeue)
        _add_thread(self._listen, args=(SELF_ADDR,))
        # start other listen thread
        for addr in self.other_addr_list:
            _add_thread(self._listen, args=(addr,))
        # wait thread
        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
 
        while active_count() > 1:
            with self.condition:
                self.stop = False
                self.condition.wait(THREAD_WAIT)
                if self.stop:
                    with self.queue_condition:
                        self.queue_stop = True
                        self.queue_condition.notify()
                    for ident in self.data.data_dict:
                        self.data.set_key(key='stop', value=True, ident=ident)

    def _signal(self,signum, frame):
        """ for signal """
        sig_names = {23:"NSIG", 22:"SIGABRT", 21:"SIGBREAK", 8:"SIGFPE", 4:"SIGILL",
                     2:"SIGINT", 11:"SIGSEGV", 15:"SIGTERM", 0:"SIG_DFL", 1:"SIG_IGN"}
        self.logger.info('received signal (%s)' % sig_names[signum])
        with self.condition:
            self.stop = True
            self.condition.notify()

    def _listen(self, addr):
        """ start to listen the self.socket continuously """
        # add socket and info
        listen_socket = _add_socket(addr)
        if not listen_socket:
            self.logger.error('listen failed @ %s' % addr)
        else:
            listen_info = {'addr':addr, 'socket':listen_socket, 'stop':False}
            self.data.set_info(listen_info)
            # listen continuously
            self.logger.info('listen start @ %s' % addr)
            while not self.data.get_value('stop'):
                try:
                    listen_socket.listen(SOCK_BACKLOG)
                    conn, conn_addr = listen_socket.accept()
                    # wait to be connected
                    self.logger.info('%s is connected by %s' % (addr,conn_addr[0]))
                except socket.timeout:
                    continue
                except socket.error, (errno, detail):
                    if not errno == 9:
                        self.logger.error('socket.error "%s"' % detail)
                    break
                # enqueue on another thread 
                enqueue_thread = _add_thread(self._enqueue, args=(conn,))
            listen_socket.close()
            self.data.del_info()
            self.logger.info('listen end @ %s' % addr)
        with self.condition:
            self.condition.notify()

    def _enqueue(self, conn):
        """ enqueue to the self.queue """
        # receive a message
        while True:
            try:
                message = conn.recv(RECV_SIZE)
                break
            except socket.error, detail:
                self.logger.error('socket.error: %s' % detail)
                continue
        # react a message
        self.react(conn, message)
        # enqueue a message
        if message.split()[0] == 'enqueue':
            queue_message = ' '.join(message.split()[1:])
            with self.queue_condition:
                self.queue.put(queue_message)
                self.queue_condition.notify()
        conn.close()
        
    def react(self, conn, message):
        """ react for the message """
        message_list = message.split()
        message_len = len(message_list)
        send_message = ''
        if message_list[0] == 'socket':
            if message_len > 1:
                if message_list[1] == 'list':
                    addr_list = []
                    for ident in self.data.data_dict:
                        ident = str(ident)
                        addr_list.append(self.data.data_dict[ident]['addr'])
                    send_message = ', '.join(addr_list)
 
                if message_len > 2:
                    if message_list[1] == 'add':
                        _add_thread(self._listen, args=(message_list[2],))
                        send_message = ('try to add socket %s' % message_list[2])
                            
                    elif message_list[1] == 'del':
                        listen_info = self.data.search_info('addr', message_list[2])
                        if listen_info:
                            listen_info['socket'].close()
                            self.logger.info('manage del socket %s' % message_list[2])
                            send_message = ('del socket %s' % message_list[2])
                        else:
                            send_message = ('not found socket %s' % message_list[2])
        conn.send(send_message)

    def _dequeue(self):
        """ dequeue from the self.queue """
        self.queue_stop = False
        # dequeu continuously
        while not self.queue_stop:
            with self.queue_condition:
                try:
                    queue_message = self.queue.get(timeout=QUEUE_WAIT)
                    addr = queue_message.split()[0]
                    message = ' '.join(queue_message.split()[0:])
                    self.queue_act(message)
                except Empty:
                    self.queue_condition.wait(COND_WAIT)
        with self.condition:
            self.stop = True
            self.condition.notify()

    def queue_act(self, message):
        """ react the message """
        self.logger.info('queu_act to message "%s"' % message)

class ListenData():
    """ info dictionary for listen """
    def __init__(self):
        """ prepare """
        self.data_condition = Condition()
        self.data_dict = {}

    def set_info(self, info):
        """ set listen info """
        ident = currentThread().ident
        with self.data_condition:
            self.data_dict[str(ident)] = info
    
    def set_key(self, key, value, ident=None):
        if not ident:
            ident = currentThread().ident
        with self.data_condition:
            self.data_dict[str(ident)][str(key)] = value

    def del_info(self):
        """ del listen info """
        ident = currentThread().ident
        with self.data_condition:
            del self.data_dict[str(ident)]

    def get_value(self, key, ident=None):
        """ set listen info """
        if not ident:
            ident = currentThread().ident
        with self.data_condition:
            return self.data_dict[str(ident)][key]
        
    def search_info(self, key, value):
        """ search info return ident """
        key = str(key)
        with self.data_condition:
            for ident in self.data_dict:
                if self.data_dict[str(ident)][key] == value:
                    return self.data_dict[str(ident)]
        return None

def _add_thread(target, args=()):
    """ start a thread """
    new_thread = Thread(target=target, args=args)
    new_thread.setDaemon(True)
    new_thread.start()
    return new_thread

def _add_socket(addr):
    """ add a socket from addr """
    host, port = _split_addr(addr)
    if not port:
        port = SELF_PORT
    # socket bind
    target_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target_socket.settimeout(SOCK_WAIT)
    try:
        target_socket.bind((host, int(port)))
    except socket.error, detail:
        sys.stderr.write('socket error %s\n' % detail)
        return None
    return target_socket

def _split_addr(addr):
    """ get host and port from addr """
    port = None
    split_addr = addr.split(':')
    host = split_addr[0]
    if len(split_addr) > 1:
        port = int(split_addr[1])
    return host, port

def speak(message_list, addr=SELF_ADDR):
    """ speak through a socket """
    # send message
    host, port = _split_addr(addr)
    if not port:
        port = SELF_PORT
    message = ' '.join(message_list)
    speak_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        speak_socket.connect((host, port))
        speak_socket.send(message)
    except socket.error, detail:
        sys.exit('socket error: "%s"' % detail)
    # accept message
    message = speak_socket.recv(RECV_SIZE)
    if message:    
        sys.stdout.write('%s\n' % message)

if __name__ == "__main__":
    pass

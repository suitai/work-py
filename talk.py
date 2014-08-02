""" talk through a socket.socket """

import os
import sys
import socket
import signal
import logging
from time import sleep
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
RECV_SIZE = 1024
QUEUE_WAIT = 0.1
SOCK_WAIT = 10
COND_WAIT = 100
THREAD_WAIT = 1000
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
        self.other_manage_list = []
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

    def set_addr(self, addr, manage=None):
        """ set an addr and a manage """
        self.other_addr_list.append(addr)
        self.other_manage_list.append(manage)

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
        _add_thread(self._listen, args=(SELF_ADDR, self.manage))
        # start other listen thread
        for addr, manage in zip(self.other_addr_list, self.other_manage_list):
            _add_thread(self._listen, args=(addr, manage))
        # wait thread
        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
 
        while active_count() > 2:
            with self.condition:
                self.is_alive = True
                self.condition.wait(THREAD_WAIT)
                if not self.is_alive:
                    self.logger.info('listen end')
                    break

    def _signal(self,signum, frame):
        """ for signal """
        sig_names = {23:"NSIG", 22:"SIGABRT", 21:"SIGBREAK", 8:"SIGFPE", 4:"SIGILL",
                     2:"SIGINT", 11:"SIGSEGV", 15:"SIGTERM", 0:"SIG_DFL", 1:"SIG_IGN"}
        self.logger.info('received signal (%s)' % sig_names[signum])
        with self.condition:
            self.is_alive = False
            self.condition.notify()

    def _listen(self, addr, manage=None):
        """ start to listen the self.socket continuously """
        # add socket and info
        listen_socket = _add_socket(addr)
        if not listen_socket:
            self.logger.error('listen failed @ %s' % addr)
            return
        listen_info = {'thread':currentThread, 'addr':addr, 'socket':listen_socket, 'manage':manage}
        self.data.set_info(listen_info)
        # listen continuously
        self.logger.info('listen start @ %s' % addr)
        while True:
            try:
                listen_socket.listen(SOCK_BACKLOG)
                conn, conn_addr = listen_socket.accept()
                # wait to be connected
                self.logger.info('%s is connected by %s:%d' % (addr,conn_addr[0],conn_addr[1]))
            except socket.timeout:
                continue
            except socket.error, (errno, detail):
                if not errno == 9:
                    self.logger.error('socket.error "%s"' % detail)
                break
            # enqueue on another thread                       
            enqueue_thread = _add_thread(self._enqueue, args=(conn,conn_addr,manage))
        listen_socket.close()
        self.data.del_info()
        self.logger.info('listen end @ %s' % addr)
        with self.condition:
            self.condition.notify()

    def _enqueue(self, conn, addr, manage):
        """ enqueue to the self.queue """
        is_enqueue = True
        # receive a message
        while True:
            try:
                message = conn.recv(RECV_SIZE)
                break
            except socket.error, detail:
                self.logger.error('socket.error: %s' % detail)
                continue
        conn.close()
        # manage before enqueue
        if manage:
            is_enqueue = manage(addr, message)
        # enqueue
        if is_enqueue:
            queue_message = ('%s %s' % (addr[0],message))
            with self.queue_condition:
                self.queue.put(queue_message)
                self.queue_condition.notify()
       
    def _dequeue(self):
        """ dequeue from the self.queue """
        # dequeu continuously
        while True:
            with self.queue_condition:
                try:
                    queue_message = self.queue.get(timeout=QUEUE_WAIT)
                    addr = queue_message.split()[0]
                    message = ' '.join(queue_message.split()[1:])
                    self.react(addr, message)
                except Empty:
                    self.queue_condition.wait(COND_WAIT)
        with self.condition:
            self.is_alive = False
            self.condition.notify()

    def manage(self, addr, message):
        """ manage the Listener """
        message_list = message.split()
        message_len = len(message_list)
        if addr[0] == SELF_HOST and message_list[0] == 'manage':
            if message_len > 3:
                if message_list[1] == 'add':
                    if message_list[2] == 'socket':
                        self.logger.info('manage add socket %s' % message_list[3])
                        _add_thread(self._listen, args=(message_list[3],))
                elif message_list[1] == 'del':
                    if message_list[2] == 'socket':
                        listen_info = self.data.search_info('addr', message_list[3])
                        if listen_info:
                            self.logger.info('manage del socket %s' % message_list[3])
                            listen_info['socket'].close()
            return False
        return True

    def react(self, addr, message):
        """ react the message """
        self.logger.info('react to message "%s" from %s' % (message, addr))

class ListenData():
    """ info dictionary for listen """
    def __init__(self):
        self.data_condition = Condition()
        self.data_dict = {}

    def get_info(self, ident=None):
        """ get listen info """
        if not ident:
            ident = currentThread().ident
        ident = str(ident)
        with self.data_condition:
            if self.datta_dict[ident]:
                return self.data_dict[ident]
        return None

    def set_info(self, info, ident=None):
        """ set listen info """
        if not ident:
            ident = currentThread().ident
        with self.data_condition:
            self.data_dict[str(ident)] = info

    def del_info(self, ident=None):
        """ del listen info """
        if not ident:    
            ident = currentThread().ident
        with self.data_condition:
            del self.data_dict[str(ident)]
        
    def search_info(self, key, value):
        """ search info return ident """
        key = str(key)
        with self.data_condition:
            for ident in self.data_dict:
                ident = str(ident)
                if self.data_dict[ident][key] == value:
                    return self.data_dict[ident]
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

if __name__ == "__main__":
    pass

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

LOG_FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
LISTEN_PORT = 50010

class Listener:
    """ listen through a socket """
    def __init__(self):
        """ prepare """
        self.condition = Condition()
        self.queue = Queue()
        self.queue_condition = Condition()
        self.data = DictData()
        self.stop = False
        self.is_daemon = False
        # make log
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        # host
        self.addr_list = ['127.0.0.1']
        host_addr = socket.gethostbyname(socket.gethostname())
        if not host_addr == '127.0.0.1':
            self.addr_list.append(host_addr)

    def add_addr(self, addr):
        """ set an addr """
        self.addr_list.append(addr)

    def daemonize(self, pidfile, logfile):
        """ deamone process """
        # check lockfile
        filelock = PIDLockFile(pidfile)
        if filelock.is_locked():
            sys.exit('%s already exists, exitting' % filelock.lock_file)
        # logfile
        handler = logging.FileHandler(logfile)
        handler.level = logging.INFO
        handler.formatter = logging.Formatter(fmt=LOG_FORMAT)
        self.logger.addHandler(handler)
        # daemonize
        self.is_daemon = True
        context = DaemonContext(pidfile=filelock, files_preserve=[handler.stream])
        with context:
            self.start()

    def start(self):
        """ start threads """
        if not self.is_daemon:
            logging.basicConfig(format=LOG_FORMAT)
        del self.is_daemon
        _add_thread(self._dequeue)
        # start other listen thread
        for addr in self.addr_list:
            _add_thread(self._listen, args=(addr,))
        del self.addr_list
        # set signal handler
        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
        # wait thread
        while active_count() > 1:
            with self.condition:
                self.condition.wait(300)
                if self.stop:
                    with self.queue_condition:
                        self.queue_stop = True
                        self.queue_condition.notify()
                    for ident in self.data.data:
                        self.data.set_value(ident, 'stop', True)

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
        ident = currentThread().ident
        # add socket and info
        host, port = _split_addr(addr)
        if not port:
            port = LISTEN_PORT
        addr = ':'.join([host,str(port)])
        # socket bind
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_socket.settimeout(3)
        try:
            listen_socket.bind((host, int(port)))
            self.logger.info('start @ %s' % addr)
        except socket.error, detail:
            self.logger.error('socket error %s\n' % detail)
            self.logger.error('listen failed @ %s' % addr)
        else:
            listen_data = {'addr':addr, 'socket':listen_socket, 'stop':False}
            self.data.set_data(ident, listen_data)
            # listen continuously
            while not self.data.get_value(ident,'stop'):
                try:
                    listen_socket.listen(1) # backlog
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
            self.data.del_data(ident)
            self.logger.info('listen end @ %s' % addr)
        finally:
            listen_socket.close()
            with self.condition:
                self.condition.notify()

    def _enqueue(self, conn):
        """ enqueue to the self.queue """
        # receive a message
        while True:
            try:
                message = conn.recv(4096) # bufsize
                break
            except socket.error, detail:
                self.logger.error('socket.error: %s' % detail)
                continue    
        # react a message
        self._react(conn, message)
        # enqueue a message
        message_list = message.split()
        if message_list.pop(0) == 'enqueue':
            queue_message = ' '.join(message_list)
            if queue_message:
                with self.queue_condition:
                    self.queue.put(queue_message)
                    self.queue_condition.notify()
        conn.close()
        
    def _react(self, conn, message):
        """ react for the message """
        message_list = message.split()
        message_len = len(message_list)
        send_message = ' '
        if message_list[0] == 'socket':
            if message_len > 1:
                if message_list[1] == 'list':
                    addr_list = []
                    for ident in self.data.data:
                        addr_list.append(self.data.data[str(ident)]['addr'])
                    send_message = '\n'.join(addr_list)
                    self.logger.info('send socket list')
        elif message_list[0] == 'queue':
            if message_len > 1:
                if message_list[1] == 'num':
                    send_message = str(self.queue.qsize())
                    self.logger.info('send queue.qsize')
                elif message_list[1] == 'list':
                    queue_list = []
                    for queue in self.queue:
                        queue_list.append(str(queue))
                    send_message = '\n'.join(queue_list)
        else:
            send_message = self.react(message)
        conn.send(send_message)

    def _dequeue(self):
        """ dequeue from the self.queue """
        self.queue_stop = False
        self.logger.info('queue start')
        # dequeu continuously
        while not self.queue_stop:
            with self.queue_condition:
                try:
                    queue_message = self.queue.get(timeout=0.3)
                    addr = queue_message.split()[0]
                    message = ' '.join(queue_message.split()[0:])
                    self.queue_act(message)
                except Empty:
                    self.queue_condition.wait(60)
        with self.condition:
            self.stop = True
            self.condition.notify()
        self.logger.info('queue end')

    def react(self, message):
        self.logger.info('react to message "%s"' % message)
        return ''

    def queue_act(self, message):
        self.logger.info('queue_act to message "%s"' % message)

class DictData():
    """ data dictionary """
    def __init__(self):
        self.data_condition = Condition()
        self.data = {}

    def set_data(self, ident, info):
        with self.data_condition:
            self.data[str(ident)] = info
    
    def set_value(self, ident, key, value):
        with self.data_condition:
            self.data[str(ident)][str(key)] = value

    def del_data(self, ident):
        with self.data_condition:
            del self.data[str(ident)]

    def get_value(self, ident, key):
        with self.data_condition:
            return self.data[str(ident)][key]

    def search_data(self, key, value):
        with self.data_condition:
            for ident in self.data:
                if self.data[str(ident)][str(key)] == value:
                    return self.data[str(ident)]
            return None

def _add_thread(target, args=()):
    """ start a thread """
    new_thread = Thread(target=target, args=args)
    new_thread.setDaemon(True)
    new_thread.start()
    return new_thread

def _split_addr(addr):
    """ get host and port from addr """
    port = None
    split_addr = addr.split(':')
    host = split_addr[0]
    if len(split_addr) > 1:
        port = int(split_addr[1])
    return host, port

def speak(message_list, addr='127.0.0.1'):
    """ speak through a socket """
    # send message
    host, port = _split_addr(addr)
    if not port:
        port = LISTEN_PORT
    message = ' '.join(message_list)
    speak_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    speak_socket.settimeout(60)
    try:
        speak_socket.connect((host, port))
        speak_socket.send(message)
    except socket.error, detail:
        speak_socket.close()
        sys.exit('socket error: "%s"' % detail)
    except socket.timeout:
        sys.exit('timeout')
    else:
        # accept message
        message = speak_socket.recv(4096) # bufsize
        if message:    
            sys.stdout.write('%s\n' % message)

if __name__ == "__main__":
    pass

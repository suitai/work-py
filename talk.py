""" talk through a socket.socket """

import os
import sys
import time
import socket
import signal
import logging
import threading
try:
    from daemon import DaemonContext
    from lockfile.pidlockfile import PIDLockFile
except ImportError, detail:
    sys.exit('ImportError: %s' % detail)

LOG_FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
LISTEN_PORT = 50011

class Listener:
    """ listen through a socket """
    def __init__(self):
        """ prepare """
        self.condition = threading.Condition()
        self.queue = QueueList()
        self.queue_condition = threading.Condition()
        self.queue_num = 1
        self.data = DataDictionary()
        self.stop = False
        self.is_daemon = False
        # make log
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        # host
        self.addr_list = ['127.0.0.1']

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
        """ start listen """
        if not self.is_daemon:
            logging.basicConfig(format=LOG_FORMAT)
        self.logger.info('-- listen start --')
        # start threads
        for ii in range(0, self.queue_num):
            _add_thread(self._dequeue)
        for addr in self.addr_list:
            _add_thread(self._listen, args=(addr,))
        # set signal handler
        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
        # wait thread
        while threading.active_count() > 1:
            with self.condition:
                self.condition.wait(300)
                if self.stop:
                    self.logger.info('-- listen end --')
                    break

    def _signal(self,signum, frame):
        """ for signal """
        sig_names = {23:"NSIG", 22:"SIGABRT", 21:"SIGBREAK", 8:"SIGFPE", 4:"SIGILL",
                     2:"SIGINT", 11:"SIGSEGV", 15:"SIGTERM", 0:"SIG_DFL", 1:"SIG_IGN"}
        self.logger.debug('received signal (%s)' % sig_names[signum])
        with self.condition:
            self.stop = True
            self.condition.notify()

    def _listen(self, addr):
        """ start to listen the self.socket continuously """
        ident = threading.currentThread().ident
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
            self.logger.error('listen failed @ [%s] %s' % (addr,detail))
        else:
            listen_data = {'addr':addr, 'socket':listen_socket, 'stop':False}
            self.data.put_data(ident, listen_data)
            # listen continuously
            while not self.data.get_value(ident,'stop'):
                try:
                    listen_socket.listen(1) # backlog
                    conn, conn_addr = listen_socket.accept()
                    # wait to be connected
                    self.logger.debug('%s is connected by %s' % (addr,conn_addr[0]))
                except socket.timeout:
                    continue
                except socket.error, (errno, detail):
                    if not errno == 9:
                        self.logger.error('socket.error "%s"' % detail)
                    break
                else:
                    # enqueue on another thread
                    enqueue_thread = _add_thread(self._enqueue, args=(conn,))
            self.data.del_data(ident)
            self.logger.info('listen end @ %s' % addr)
        finally:
            listen_socket.close()
            with self.condition:
                self.condition.notify()

    def _react(self, conn, message):
        """ react for the message """
        message_list = message.split()
        message_len = len(message_list)
        send_message = ''
        if message_list[0] == 'socket':
            send_message = 'Usage: socket [list]'
            if message_len > 1:
                if message_list[1] == 'list':
                    addr_list = self.data.list_value('addr')
                    send_message = '\n'.join(addr_list)
                    self.logger.info('send socket list')
        elif message_list[0] == 'queue':
            send_message = 'Usage: queue [num|list]'
            if message_len > 1:
                if message_list[1] == 'num':
                    send_message = str(len(self.queue.queue))
                    self.logger.info('send queue size')
                elif message_list[1] == 'list':
                    item_list = self.queue.list_item()
                    send_message = '\n'.join(item_list)
                    self.logger.info('send queue list')
        else:
            send_message = self.react(message)
        conn.send(send_message)

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
            num = None
            queue_message = ' '.join(message_list)
            if queue_message:
                with self.queue_condition:
                    num = self.queue.put_item(queue_message)
                    self.queue_condition.notify()
                if num:
                    self.enqueue_act(num, queue_message)
        conn.close()

    def _dequeue(self):
        """ dequeue from the self.queue """
        self.queue_stop = False
        self.logger.info('queue start')
        # dequeu continuously
        while not self.queue_stop:
            num = None
            queue_message = None
            with self.queue_condition:
                try:
                    num, queue_message = self.queue.get_item()
                except QueueListEmpty:
                    self.queue_condition.wait(60)
            if num:
                self.dequeue_act(num, queue_message)
        with self.condition:
            self.stop = True
            self.condition.notify()
        self.logger.info('queue end')

    def react(self, message):
        self.logger.info('react to message "%s"' % message)
        return ''

    def enqueue_act(self, num, message):
        self.logger.info('enqueue_act to [%d] message "%s"' % (num, message))

    def dequeue_act(self, num, message):
        self.logger.info('dequeue_act to [%d] message "%s"' % (num, message))
        time.sleep(40)

class DataDictionary():
    """ self data dictionary """
    def __init__(self):
        self.condition = threading.Condition()
        self.data = {}

    def put_data(self, ident, info):
        with self.condition:
            self.data[str(ident)] = info
    
    def set_value(self, ident, key, value):
        with self.condition:
            self.data[str(ident)][str(key)] = value

    def del_data(self, ident):
        with self.condition:
            del self.data[str(ident)]

    def get_value(self, ident, key):
        with self.condition:
            return self.data[str(ident)][key]

    def list_value(self, key):
        value_list = []
        with self.condition:
            for ident in self.data:
                value_list.append(self.data[str(ident)][key])
        return value_list

    def search_data(self, key, value):
        with self.condition:
            for ident in self.data:
                if self.data[str(ident)][str(key)] == value:
                    return self.data[str(ident)]
            return None

class QueueListEmpty(Exception):
        pass

class QueueList():
    """ self queue list """
    def __init__(self):
        self.condition = threading.Condition()
        self.queue = []
        self.num = 0
        self.last = 0
        self.num_list = []

    def put_item(self, item):
        with self.condition:
            self.num = self.num + 1
            self.num_list.append(self.num)
            self.queue.append(str(item))
        return self.num

    def get_item(self):
        with self.condition:
            try: 
                num = self.num_list.pop(0)
                item = self.queue.pop(0)
            except IndexError:
                raise QueueListEmpty, 'the queue list is empty'
        return num, item

    def list_item(self):
        item_list = []
        for num,item in zip(self.num_list,self.queue):
            item_list.append('%s %s' %(str(num).zfill(3),item))
        return item_list 

def _add_thread(target, args=()):
    """ start a thread """
    new_thread = threading.Thread(target=target, args=args)
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
        sys.exit('socket error: [%s:%d] %s ' % (host,port,detail))
    except socket.timeout:
        speak_socket.close()
        sys.exit('timeout')
    else:
        # accept message
        message = speak_socket.recv(4096) # bufsize
        if message:    
            sys.stdout.write('%s\n' % message)

if __name__ == '__main__':
    pass

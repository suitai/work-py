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

LOG_FORMAT = "%(asctime)-15s %(filename)s %(levelname)s %(message)s"
LISTEN_PORT = 50010
IDFILE = '/var/run/queue.id'

class Listener:
    """ listen through a socket """
    def __init__(self):
        """ prepare """
        # make data
        self.condition = threading.Condition()
        self.queue_data = DataDictionary()
        self.socket_data = DataDictionary()
        self.ident = IdentInt(idfile=IDFILE)
        # make log
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        # initialize value
        self.queue_list = ['default']
        self.socket_list = ['127.0.0.1']
        self.is_daemon = False
        self.stop = False

    def add_queue(self, name):
        """ add queue before start """
        self.queue_list.append(name)

    def add_socket(self, addr):
        """ add queue before start """
        self.socket_list.append(addr)

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
        for name in self.queue_list:
            _add_thread(self._dequeue, args=(name,))
        for addr in self.socket_list:
            _add_thread(self._listen, args=(addr,))
        # set signal handler
        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
        # wait thread
        while threading.active_count() > 2:
            with self.condition:
                self.condition.wait(300)
                if self.stop:
                    self.logger.info('-- listen end --')
                    break

    def _signal(self, signum, frame):
        """ for signal """
        sig_names = {23:"NSIG", 22:"SIGABRT", 21:"SIGBREAK", 8:"SIGFPE", 4:"SIGILL",
                     2:"SIGINT", 11:"SIGSEGV", 15:"SIGTERM", 0:"SIG_DFL", 1:"SIG_IGN"}
        self.logger.debug('received signal (%s)' % sig_names[signum])
        with self.condition:
            self.stop = True
            self.condition.notify()

    def _listen(self, addr):
        """ start to listen the socket continuously """
        ident = threading.currentThread().ident
        (host, port) = _split_addr(addr)
        if not port:
            port = LISTEN_PORT
        addr = ':'.join([host,str(port)])
        # add socket
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_socket.settimeout(3)
        try:
            listen_socket.bind((host, int(port)))
            self.logger.info('listen start @ %s' % addr)
        except socket.error, detail:
            self.logger.error('listen failed @ %s because %s' % (addr,detail))
        else:
            # put socket info
            socket_info = {'addr':addr, 'socket':listen_socket, 'status':'Start'}
            self.socket_data.put_info(ident, socket_info)
            # listen continuously
            while not self.socket_data.get_value(ident,'status') == 'Stop':
                try:
                    self.socket_data.set_value(ident, 'status', 'Listen')
                    listen_socket.listen(1) # backlog
                    (conn, conn_addr) = listen_socket.accept()
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
                    self.socket_data.set_value(ident, 'status', 'Enqueue')
                    enqueue_thread = _add_thread(self._enqueue, args=(conn,))
            self.socket_data.del_info(ident)
            self.logger.info('listen end @ %s' % addr)
        finally:
            listen_socket.close()
            with self.condition:
                self.condition.notify()

    def _enqueue(self, conn):
        """ enqueue to the queue """
        # receive a message
        is_error = False
        while True:
            try:
                message = conn.recv(4096) # bufsize
                message_list = message.split()
                message_len = len(message_list)
                conn_addr = conn.getsockname()
                self.logger.debug('from %s:%d get message "%s"' % (conn_addr[0],conn_addr[1],message))
                break
            except socket.error, detail:
                if not is_error:
                    self.logger.error('socket.error: %s' % detail)
                    is_error = True
                continue
        # check a message
        reply_message = ''
        queue_message = None
        if message_list[0] == 'socket':
            # socket info
            reply_message = 'Usage: socket [list]'
            if message_len > 1:
                if message_list[1] == 'list':
                    addr_list = self.socket_data.list_value('addr')
                    reply_message = '\n'.join(addr_list)
                    self.logger.info('send socket list')
        elif message_list[0] == 'queue':
            # queue info
            reply_message = 'Usage: queue [list]'
            if message_len > 1:
                if message_list[1] == 'list':
                    name_list = []
                    for name in self.queue_data.list_value('name'):
                        name_list.append(name)
                        reply_message = '\n'.join(name_list)
                        self.logger.info('send queue list')
        elif message_list[0] == 'enqueue':
            # enqueue check
            reply_message = 'Usage: enqueue <queue> <message>'
            if len(message_list) > 1:
                queue_name = message_list[1]
                if queue_name in self.queue_data.list_value('name'):
                    queue_info = self.queue_data.search_info('name', queue_name)
                    if queue_info:
                        queue = queue_info['queue']
                        queue_condition = queue_info['condition']
                        queue_ident = self.ident.get()
                        queue_message = ' '.join(message_list[2:])
                    reply_message = ('enqueue to "%s" %d' % (queue_name,queue_ident))
                else:
                    send_message = ('cannot find "%s"' % queue_name)
        # editable react function
        new_message = self.react(message)
        if new_message:
            reply_message = new_message
        # reply message
        conn.send(reply_message)
        # enqueue message
        if queue_message:
            with queue_condition:
                queue.put_item(queue_ident, queue_message)
                # editable enqueue_act function
                self.enqueue_act(queue_ident, queue_name, queue_message)
                queue_condition.notify()
        conn.close()

    def _dequeue(self, queue_name):
        """ dequeue from the self.queue """
        # put queue info
        ident = threading.currentThread().ident
        queue = QueueList()
        queue_condition = threading.Condition()
        queue_info = {'name':queue_name, 'queue':queue, 'condition':queue_condition, 'status':'Start'}
        self.queue_data.put_info(ident, queue_info)
        self.logger.info('"%s" queue start' % queue_name)
        # queue check continuously
        while not self.queue_data.get_value(ident,'status') == 'Stop':
            queue_message = None
            message_ident = None
            with queue_condition:
                try:
                    message_ident, queue_message = queue.get_item()
                    self.logger.debug('from "%s" get message "%s"' % (queue_name,queue_message))
                except QueueListEmpty:
                    self.queue_data.set_value(ident, 'status', 'Empty')
                    queue_condition.wait(60)
            if message_ident:
                # editable dequeue_act function
                self.queue_data.set_value(ident, 'status', 'Dequeue')
                self.dequeue_act(message_ident, queue_name, queue_message)
        with self.condition:
            self.condition.notify()
        self.queue_data.del_info(ident)
        self.logger.info('"%s" queue end' % queue_name)

    def react(self, message):
        self.logger.info('react to message "%s"' % message)
        return ''

    def enqueue_act(self, ident, name, message):
        self.logger.info('enqueue to %s message [%d] "%s"' % (name, ident, message))

    def dequeue_act(self, ident, message):
        self.logger.info('dequeue from %s message [%d] "%s"' % (name, ident, message))
        time.sleep(30)

class DataDictionary():
    """ self data dictionary """
    def __init__(self):
        self.condition = threading.Condition()
        self.data = {}

    def put_info(self, ident, info):
        with self.condition:
            self.data[str(ident)] = info
    
    def set_value(self, ident, key, value):
        with self.condition:
            self.data[str(ident)][str(key)] = value

    def del_info(self, ident):
        with self.condition:
            del self.data[str(ident)]

    def get_info(self, ident):
        with self.condition:
            return self.data[str(ident)]

    def get_value(self, ident, key):
        with self.condition:
            return self.data[str(ident)][key]

    def list_value(self, key):
        value_list = []
        with self.condition:
            for ident in self.data:
                value_list.append(self.data[str(ident)][key])
        return value_list

    def search_info(self, key, value):
        with self.condition:
            for ident in self.data:
                if self.data[str(ident)][str(key)] == value:
                    return self.data[str(ident)]
        return None

class QueueListEmpty(Exception):
        pass

class IdentInt():
    def __init__(self, idfile=None):
        self.idfile = None
        self.ident = 0
        self.condition = threading.Condition()
        if idfile:
            self.idfile = idfile
            if os.path.exists(self.idfile):
                try:
                    file = open(self.idfile, 'r')
                    self.ident = int(file.read())
                except ValueError:
                    pass
                finally:
                    file.close()
            else:
                file = open(self.idfile, 'w')
                file.write(str(self.ident))
                file.close()
            os.chmod(self.idfile, 0666)

    def get(self):
        self.ident = self.ident + 1
        if self.idfile:
            file = open(self.idfile, 'w')
            file.write(str(self.ident))
            file.close()
        return self.ident

class QueueList():
    """ self queue list """
    def __init__(self):
        self.condition = threading.Condition()
        self.queue = []
        self.ident_list = []

    def put_item(self, ident, item):
        with self.condition:
            self.ident_list.append(ident)
            self.queue.append(str(item))
        return ident

    def get_item(self):
        with self.condition:
            try: 
                ident = self.ident_list.pop(0)
                item = self.queue.pop(0)
            except IndexError:
                raise QueueListEmpty, 'the queue list is empty'
        return ident, item

    def list_item(self):
        item_list = []
        for ident,item in zip(self.ident_list,self.queue):
            item_list.append('%s %s' %(str(ident).zfill(3),item))
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
    addr_list = str(addr).split(':')
    host = addr_list[0]
    if len(addr_list) > 1:
        port = int(addr_list[1])
    return host, port

def speak(message, addr='127.0.0.1'):
    """ speak through a socket """
    if len(message) > 4096:
        sts.exit('message is too large')
    # send message
    (host, port) = _split_addr(addr)
    if not port:
        port = LISTEN_PORT
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

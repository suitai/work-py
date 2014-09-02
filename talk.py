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

class TalkManager:
    """ listen through a socket """
    def __init__(self, idfile=IDFILE):
        """ prepare """
        # make data
        self.condition = threading.Condition()
        self.queue_condition = threading.Condition()
        self.list_condition = threading.Condition()
        self.ident = TalkIdent(idfile=idfile)
        self.sockets = {}
        self.queues = {}
        self.listeners = {}
        self.workers = {}
        self.status = 'Start'
        self.queue_names = []
        self.socket_addrs = []
        self.worker_names = []
        # make log
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
  
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
        self.status = 'Daemon'
        context = DaemonContext(pidfile=filelock, files_preserve=[handler.stream])
        with context:
            self.start()

    def start(self):
        """ start listen """
        if not self.status == 'Daemon':
            logging.basicConfig(format=LOG_FORMAT)
        self.logger.info('-- listen start --')
        # start threads
        if not self.queue_names: self.queue_names.append('05_default')
        if not self.socket_addrs: self.socket_addrs.append('127.0.0.1')
        if not self.worker_names: self.worker_names.append('Jonathan')
        for name in self.queue_names: self.add_queue(name)
        for addr in self.socket_addrs: self.add_listener(addr)
        for name in self.worker_names: self.add_worker(name)
        # set signal handler
        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
        # wait thread
        while (len(self.workers) > 0 and len(self.listeners) > 0
               and (not self.status=='Stop')):
            with self.condition:
                self.condition.wait(300)
        # del all queues
        for name in self.workers.keys(): self.del_worker(name)
        for addr in self.workers.keys(): self.del_listener(addr)
        for name in self.queues.keys(): self.del_queue(name)
        self.logger.info('-- listen end --')

    def _signal(self, signum, frame):
        """ for signal """
        sig_names = {23:"NSIG", 22:"SIGABRT", 21:"SIGBREAK", 8:"SIGFPE", 4:"SIGILL",
                     2:"SIGINT", 11:"SIGSEGV", 15:"SIGTERM", 0:"SIG_DFL", 1:"SIG_IGN"}
        self.logger.info('received signal (%s)' % sig_names[signum])
        self.status = 'Stop'
        with self.condition:
            self.condition.notify()

    def _listen(self, socket_addr):
        """ start to listen the socket continuously """
        # listen thread info
        talk_listener = TalkListener(socket_addr)
        socket_addr = talk_listener.addr
        with self.list_condition:
            self.listeners[socket_addr] = talk_listener
        self.logger.info('listener "%s" start' % socket_addr)
        # listen continuously
        while not self.listeners[socket_addr].status == 'Stop':
            # listen
            try:
                self.listeners[socket_addr].status = 'Listen'
                (conn, conn_addr) = self.sockets[socket_addr].listen()
                if self.listeners[socket_addr].status == 'Stop':
                    break
                self.listeners[socket_addr].status = 'Receive'
                enqueue_thread = _add_thread(self._check, args=(conn,conn_addr,))
                self.logger.debug('"%s" is connected by "%s:%d"' 
                                  % (socket_addr,conn_addr[0],conn_addr[1]))
            except TalkError, detail:
                self.logger.error('socket error @ "%s" because "%s"' 
                                  % (socket_addr,detail))
                break
            except KeyError:
                self.logger.error('cannot find the socket "%s"' % socket_addr)
                break
        # end
        with self.list_condition:
            del self.listeners[socket_addr]
        with self.condition:
            self.condition.notifyAll()
        self.logger.info('listener end @ %s' % socket_addr)

    def _check(self, conn, conn_addr):
        """ get a message and check """
        # receive a message
        is_error = False
        for num in range(0,9):
            try:
                message = conn.recv(4096) # bufsize
                self.logger.debug('from %s:%d get message "%s"' 
                                  % (conn_addr[0],conn_addr[1],message))
                break
            except socket.error, detail:
                if not is_error:
                    self.logger.error('socket.error: %s' % detail)
                    is_error = True
                continue
        # check a message
        messages = message.split()
        reply_message = self.check_messages(messages)
        # reply message
        conn.send(reply_message)
        conn.close()

    def _work(self, worker_name):
        """ dequeue from queue and work """
        # work thread info
        talk_worker = TalkWorker(worker_name)
        with self.list_condition:
            self.workers[worker_name] = talk_worker
        self.logger.info('worker "%s" start' % worker_name)
        # get item from queue continuously
        while not self.workers[worker_name].status == 'Stop':
            self.workers[worker_name].status = 'Wait'
            queue_message = None
            with self.queue_condition:
                for name in sorted(self.queues):
                    (queue_message, message_ident) = self.queues[name].get_item()
                    break
                else:
                    self.queue_condition.wait(300)
            if queue_message:
                self.logger.debug('worker "%s" get message "%s" from "%s"'
                                   % (worker_name,queue_message,name))
                self.workers[worker_name].status = ('Work %d' % message_ident)
                self.queue_work(name, queue_message, message_ident)
       # end
        with self.list_condition:
            del self.workers[worker_name]
        with self.condition:
            self.condition.notifyAll()
        self.logger.info('worker "%s" end' % worker_name)

    def check_messages(self, messages):
        reply_message = ''
        if len(messages) > 1:
            queue_name = messages[0]
            queue_message = ' '.join(messages[1:])
            ident = self.enqueue_message(queue_name, queue_message)
            if ident > 0:
                reply_message = ('%d' % ident)
        return reply_message

    def queue_work(self, queue_name, queue_message, message_ident=0):
        self.logger.info('dequeue from %s message [%d] "%s"'
                         % (queue_name,message_ident,queue_message))
        time.sleep(30)

    def add_queue(self, queue_name):
        """ add queue """
        for name in self.queues:
            if name == queue_name:
                self.logger.warn('"%s" queue already exists' % queue_name)
                return False
        talk_queue = TalkQueue(queue_name)
        with self.queue_condition:
            with self.list_condition:
                self.queues[queue_name] = talk_queue
        self.logger.info('"%s" queue start' % queue_name)
        return True

    def del_queue(self, queue_name):
        """ delete queue """
        with self.queue_condition:
            try:
                with self.list_condition:
                    del self.queues[queue_name]
                self.logger.info('"%s" queue end' % queue_name)
                return True
            except KeyError:
                self.logger.warn('cannot found "%s" queue' % queue_name)
                return False

    def add_listener(self, socket_addr):
        """ add listener """
        (host, port) = _split_addr(socket_addr)
        socket_addr = ':'.join([host,str(port)])
        for addr in self.listeners:   
            if addr == socket_addr:
                self.logger.warn('"%s" listener already exists' % socket_addr)
                return False
        with self.list_condition:
            try:
                talk_socket = TalkSocket(socket_addr)
                self.sockets[socket_addr] = talk_socket
                _add_thread(self._listen, args=(socket_addr,))
                return True
            except TalkError, detail:
                self.logger.warn('"%s" socket failed to start because %s' % (addr,detail))
                return False

    def del_listener(self, socket_addr):
        """ del listener """
        (host, port) = _split_addr(socket_addr)
        socket_addr = ':'.join([host,str(port)])
        with self.condition:
            try:
                self.listeners[socket_addr].status = 'Stop'
                self.sockets[socket_addr].stop()
                self.condition.wait(9)
                del self.sockets[socket_addr]
                return True
            except KeyError:
                self.logger.warn('cannot find the "%s" listener' % socket_addr)
                return False

    def add_worker(self, worker_name):
        """ add worker """
        for name in self.workers:
            if  name == worker_name:
                self.logger.warn('"%s" worker already exists' % worker_name)
                return False
        _add_thread(self._work,args=(worker_name,))
        return True

    def del_worker(self, worker_name):
        """ del worker """
        with self.condition:
            try:
                self.workers[worker_name].status = 'Stop'
                with self.queue_condition:
                    self.queue_condition.notifyAll()
                self.condition.wait(9)
                return True
            except KeyError:
                self.logger.warn('cannot find the "%s" worker' % name)
                return False

    def enqueue_message(self, queue_name, queue_message):
        """ enqueue message """
        with self.queue_condition:
            for name in self.queues:
                if name == queue_name:
                    ident = self.ident.get_ident()
                    self.queues[queue_name].put_item(ident, queue_message)
                    self.queue_condition.notifyAll()
                    self.logger.info('enqueue "%s" for "%s"' 
                                  % (queue_message,queue_name))
                return ident
        self.logger.warn('cannot find the "%s" worker' % queue_name)
        return -1 

def speak(message, addr='127.0.0.1'):
    """ speak through a socket """
    if len(message) > 4096:
        sts.exit('message is too large')
    # send message
    (host, port) = _split_addr(addr)
    speak_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    speak_socket.settimeout(300)
    try:
        speak_socket.connect((host,port))
        speak_socket.send(message)
    except socket.error, detail:
        speak_socket.close()
        sys.exit('socket error: [%s:%d] %s ' % (host,port,detail))
    except socket.timeout:
        speak_socket.close()
        sys.exit('socket timeout')
    else:
        # accept message
        message = speak_socket.recv(4096) # bufsize
        return message

class TalkError(Exception):
    pass

class TalkQueue:
    def __init__(self, queue_name):
        self.name = queue_name
        self.condition = threading.Condition()
        self.items = []
        self.idents = []
        self.status = 'Empty'

    def put_item(self, ident, item):
        with self.condition:
            self.idents.append(ident)
            self.items.append(str(item))
        self.status = ('Remain %d' % len(self.items))

    def get_item(self):
        with self.condition:
            if len(self.items) > 0:
                ident = self.idents.pop(0)
                item = self.items.pop(0)
            else:
                ident = None
                item = None
                self.status = 'Empty'
        return (item, ident)

    def del_item(self, target_ident):
        for (num, ident) in enumerate(self.idents):
            if ident == target_ident:
                with self.condition:
                    del self.idents[num]
                    del self.items[num]
                return True
        return False

class TalkSocket:
    def __init__(self, socket_addr):
        (self.host, self.port) = _split_addr(socket_addr)
        self.addr = socket_addr
        self.condition = threading.Condition()
        # add socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.settimeout(300)
        try:
            self.socket
            self.socket.bind((self.host, int(self.port)))
        except socket.error, (errno,detail):
            raise TalkError(detail)

    def listen(self):
        conn = None
        conn_addr = None
        with self.condition:
            while not conn:
                try:
                    self.socket.listen(1) # backlog
                    (conn,conn_addr) = self.socket.accept()
                    # wait to be connected
                except socket.timeout:
                    continue
                except socket.error, (errno,detail):
                    if not errno == 9: # EBADF
                        self.socket.close()
                        raise TalkError(detail)
        return (conn, conn_addr)

    def stop(self):
        tmp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmp_socket.connect((self.host,int(self.port)))
        self.socket.close()

class TalkListener:
    def __init__(self, socket_addr):
        (host, port) = _split_addr(socket_addr)
        socket_addr = ':'.join([host,str(port)])
        self.addr = socket_addr
        self.status = 'Start'

class TalkWorker:
    def __init__(self, worker_name):
        self.name = worker_name
        self.status = 'Start'

class TalkIdent():
    def __init__(self, idfile=None):
        self.idfile = None
        self.ident = 1 
        self.condition = threading.Condition()
        if idfile:
            self.idfile = idfile
            if os.path.exists(self.idfile):
                file = open(self.idfile, 'r')
                try:
                    self.ident = int(file.read())
                except ValueError:
                    self.ident = 1
                file.close()
                if not self.ident > 0:
                    self.ident = 1
            else:
                file = open(self.idfile, 'w')
                file.write(str(self.ident))
                file.close()
            os.chmod(self.idfile, 0666)

    def get_ident(self):
        self.ident = self.ident + 1
        if self.idfile:
            file = open(self.idfile, 'w')
            file.write(str(self.ident))
            file.close()
        return self.ident

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
    else:
        port = LISTEN_PORT
    return (host, port)

if __name__ == '__main__':
    pass

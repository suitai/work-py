""" talk through a socket.socket """

import os
import sys
import time
import socket
import signal
import logging
import threading
from daemon import DaemonContext
from lockfile.pidlockfile import PIDLockFile


class Conversation(object):
    """
    listen and speak through a socket,
    check messages and respond,
    queue and dequeue messages to sequencial work.
    """
    buffer_size = 4096
    log_format = "%(asctime)-15s %(filename)s %(levelname)s %(message)s"
    idfile = '/var/run/queue.id'
    sig_names = {23: "NSIG",
                 22: "SIGABRT",
                 21: "SIGBREAK",
                 8: "SIGFPE",
                 4: "SIGILL",
                 2: "SIGINT",
                 11: "SIGSEGV",
                 15: "SIGTERM",
                 0: "SIG_DFL",
                 1: "SIG_IGN"}

    def __init__(self):
        """ prepare """
        self.status = 'Init'
        # make log
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

    def speak(self, message, addr='127.0.0.1'):
        """ speak through a socket """
        if len(message) > self.buffer_size:
            raise TalkError('message is too large')
        # send message
        (host, port) = _split_addr(addr)
        speak_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        speak_socket.settimeout(300)
        try:
            speak_socket.connect((host, port))
            speak_socket.send(message)
        except socket.error, detail:
            speak_socket.close()
            raise TalkError('socket.error: %s' % detail)
        except socket.timeout:
            speak_socket.close()
            raise TalkError('socket.timeout')
        else:
            # accept message
            message = speak_socket.recv(self.buffer_size)  # bufsize
            return message

    def prepare_listen(self, idfile=None):
        """ before start listen """
        self.condition = threading.Condition()
        self.queue_condition = threading.Condition()
        self.list_condition = threading.Condition()
        if not idfile:
            idfile = self.idfile
        self.ident = TalkIdent(idfile)
        self.queue_info = []
        self.socket_info = []
        self.worker_info = []
        self.sockets = {}
        self.queues = {}
        self.listeners = {}
        self.workers = {}

    def daemonize_listen(self, pidfile, logfile):
        """ deamone process """
        # check lockfile
        lock = PIDLockFile(pidfile)
        if lock.is_locked():
            raise TalkError('%s already exists' % lock.lock_file)
        # logfile
        handler = logging.FileHandler(logfile)
        handler.level = logging.INFO
        handler.formatter = logging.Formatter(fmt=self.log_format)
        self.logger.addHandler(handler)
        # daemonize
        self.status = 'Daemonize'
        context = DaemonContext(pidfile=lock, files_preserve=[handler.stream])
        with context:
            self.start_listen()

    def start_listen(self):
        """ start listen """
        if not self.status == 'Daemonize':
            logging.basicConfig(format=self.log_format)
            self.status = 'Init'
        # start
        self.logger.info('-- listen start --')
        self.status = 'Start'
        # other queues and threads start
        for queue_name in self.queue_info:
            self.add_queue(queue_name)
        for socket_addr, check_fn in self.socket_info:
            self.add_listener(socket_addr, check_fn)
        for worker_name, work_fn in self.worker_info:
            self.add_worker(worker_name, work_fn)
        # set signal handler
        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
        # wait thread
        while (len(self.workers) > 0 and len(self.listeners) > 0
               and (not self.status == 'Stop')):
            with self.condition:
                self.condition.wait(300)
        # del all queues and threads
        for worker_name in self.workers.keys():
            self.del_worker(worker_name)
        for socket_addr in self.listeners.keys():
            self.del_listener(socket_addr)
        for queue_name in self.queues.keys():
            self.del_queue(queue_name)
        self.logger.info('-- listen end --')

    def add_queue(self, queue_name):
        """ add queue """
        if self.status == 'Init':
            self.queue_info.append(queue_name)
            return True
        else:
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

    def _signal(self, signum, frame):
        """ respond to signal """
        self.logger.info('received signal (%s)' % self.sig_names[signum])
        self.status = 'Stop'
        with self.condition:
            self.condition.notify()

    def add_listener(self, socket_addr, check_fn=None):
        """ add listener """
        if self.status == 'Init':
            self.socket_info.append((socket_addr, check_fn))
            return True
        else:
            if not check_fn:
                check_fn = self.check_fn
            (host, port) = _split_addr(socket_addr)
            socket_addr = ':'.join([host, str(port)])
            for addr in self.listeners:
                if addr == socket_addr:
                    self.logger.warn('"%s" listener already exists'
                                     % socket_addr)
                    return False
            listen_socket = ListenSocket(socket_addr)
            with self.list_condition:
                self.sockets[socket_addr] = listen_socket
            try:
                _add_thread(self._listen, args=(socket_addr, check_fn,))
                return True
            except TalkError, detail:
                self.logger.warn('"%s" socket failed to start because %s'
                                 % (addr, detail))
                return False

    def del_listener(self, socket_addr):
        """ del listener """
        (host, port) = _split_addr(socket_addr)
        socket_addr = ':'.join([host, str(port)])
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

    def _listen(self, socket_addr, check_fn):
        """ listen the socket """
        # listen thread info
        talk_listener = TalkListener(socket_addr, check_fn)
        socket_addr = talk_listener.addr
        with self.list_condition:
            self.listeners[socket_addr] = talk_listener
            self.listeners[socket_addr].status = 'Start'
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
                self.logger.info('"%s" is connected by "%s:%d"'
                                 % (socket_addr, conn_addr[0], conn_addr[1]))
                self._spawn_checker(conn, conn_addr, socket_addr)
            except TalkError, detail:
                self.logger.error('socket error @ "%s" because "%s"'
                                  % (socket_addr, detail))
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

    def _spawn_checker(self, conn, conn_addr, socket_addr):
        """ spawn a checker thread """
        _add_thread(self._check, args=(conn, conn_addr, socket_addr))
        self.logger.info('checker start from "%s"' % socket_addr)
        return True

    def _check(self, conn, conn_addr, socket_addr):
        """ get a message and check """
        # receive a message
        is_error = False
        for num in range(0, 9):
            try:
                message = conn.recv(self.buffer_size)
                self.logger.debug('from %s:%d get message "%s"'
                                  % (conn_addr[0], conn_addr[1], message))
                break
            except socket.error, detail:
                if not is_error:
                    self.logger.error('socket.error: %s' % detail)
                    is_error = True
                continue
        # check a message
        reply = self.listeners[socket_addr].check_fn(message)
        # reply message
        conn.send(reply)
        conn.close()

    def check_fn(self, message):
        """ check instruction """
        reply = ''
        messages = message.split()
        if len(messages) > 1:
            queue_name = messages[0]
            message = ' '.join(messages[1:])
            message_id = self.enqueue_message(queue_name, message)
            if message_id > 0:
                reply = ('%d' % message_id)
        return reply

    def enqueue_message(self, queue_name, message):
        """ enqueue message """
        with self.queue_condition:
            for name in self.queues:
                if name == queue_name:
                    message_id = self.ident.get_ident()
                    try:
                        self.ident.write_ident()
                    except IOError:
                        self.logger.warn('cannot write the ident')
                    self.queues[queue_name].put_item(message_id, message)
                    self.queue_condition.notifyAll()
                    self.logger.debug('enqueue [%d] for "%s"'
                                      % (message_id, queue_name))
                    return message_id
        self.logger.warn('cannot find the "%s" queue' % queue_name)
        return -1

    def add_worker(self, worker_name, work_fn=None):
        """ add worker """
        if self.status == 'Init':
            self.worker_info.append((worker_name, work_fn))
            return True
        else:
            if not work_fn:
                work_fn = self.work_fn
            for name in self.workers:
                if name == worker_name:
                    self.logger.warn('"%s" worker already exists'
                                     % worker_name)
                    return False
            _add_thread(self._work, args=(worker_name, work_fn,))
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
                self.logger.warn('cannot find the "%s" worker' % worker_name)
                return False

    def _work(self, worker_name, worker_fn):
        """ dequeue from a queue and work """
        # work thread info
        talk_worker = TalkWorker(worker_name, worker_fn)
        with self.list_condition:
            self.workers[worker_name] = talk_worker
            self.workers[worker_name].status = 'Start'
        self.logger.info('worker "%s" start' % worker_name)
        # get item from queue continuously
        while not self.workers[worker_name].status == 'Stop':
            self.workers[worker_name].status = 'Wait'
            message = None
            with self.queue_condition:
                for queue_name in sorted(self.queues.keys()):
                    (message, message_id) = self.queues[queue_name].get_item()
                    if message:
                        break
                else:
                    self.queue_condition.wait(300)
            if message:
                self.logger.info('worker "%s" get message[%d] from "%s"'
                                 % (worker_name, message_id, queue_name))
                self.workers[worker_name].status = ('Work %s' % message_id)
                self.workers[worker_name].work = message_id
                self.workers[worker_name].work_fn(queue_name, message,
                                                  message_id)
        # end
        with self.list_condition:
            del self.workers[worker_name]
        with self.condition:
            self.condition.notifyAll()
        self.logger.info('worker "%s" end' % worker_name)

    def work_fn(self, queue_name, message, message_id):
        """ work instraction """
        self.logger.info('dequeue from %s message[%d]'
                         % (queue_name, message_id))
        time.sleep(30)


class TalkError(Exception):
    pass


class TalkQueue:
    """ queue definition """

    def __init__(self, queue_name, worker=None):
        self.name = queue_name
        self.condition = threading.Condition()
        self.items = []
        self.idents = []
        self.worker = []
        self.status = 'Empty'
        if worker:
            worker.append(worker)

    def put_item(self, ident, item):
        """ put a item to the queue """
        with self.condition:
            self.idents.append(ident)
            self.items.append(str(item))
            self.status = ('Remain %s' % len(self.items))

    def get_item(self):
        """ get the item from the queue """
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
        """ delete the item from the queue """
        for (num, ident) in enumerate(self.idents):
            if ident == target_ident:
                with self.condition:
                    del self.idents[num]
                    del self.items[num]
                    self.status = ('Remain %s' % len(self.items))
                return True
        return False


class ListenSocket:
    """ make socket and listen """

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
        except socket.error, (errno, detail):
            raise TalkError('socket.error: %s' % detail)

    def listen(self):
        """ listen the socket """
        conn = None
        conn_addr = None
        self.status = 'Start'
        with self.condition:
            while not conn:
                try:
                    self.socket.listen(1)  # backlog
                    (conn, conn_addr) = self.socket.accept()
                    # wait to be connected
                except socket.timeout:
                    continue
                except socket.error, (errno, detail):
                    if not errno == 9:  # EBADF
                        self.socket.close()
                        raise TalkError('socket.error: %s' % detail)
        return (conn, conn_addr)

    def stop(self):
        """ stop the socket """
        tmp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmp_socket.connect((self.host, int(self.port)))
        self.socket.close()


class TalkListener:
    """ listener definition """

    def __init__(self, socket_addr, check_fn=Conversation.check_fn):
        (host, port) = _split_addr(socket_addr)
        socket_addr = ':'.join([host, str(port)])
        self.check_fn = check_fn
        self.addr = socket_addr
        self.status = 'Init'


class TalkWorker:
    """ worker definition """

    def __init__(self, worker_name, work_fn=Conversation.work_fn):
        self.name = worker_name
        self.work_fn = work_fn
        self.work = 0
        self.status = 'Init'


class TalkIdent():
    """ manage sequencial ident """

    def __init__(self, idfile):
        self.idfile = idfile
        self.ident = 1
        self.condition = threading.Condition()
        if os.path.exists(self.idfile):
            with open(self.idfile, 'r') as f:
                try:
                    self.ident = int(f.read())
                except ValueError:
                    self.ident = 1
            if not self.ident > 0:
                self.ident = 1
        else:
            with open(self.idfile, 'w') as f:
                f.write(str(self.ident))
        os.chmod(self.idfile, 0666)

    def get_ident(self):
        """ get ident and set next ident """
        self.ident = self.ident + 1
        if os.path.exists(self.idfile):
            with open(self.idfile, 'w') as f:
                f.write(str(self.ident))
        return self.ident

    def write_ident(self):
        """ write ident to the file """
        try:
            file = open(self.idfile, 'w')
            file.write(str(self.ident))
            file.close()
        except IOError, detail:
            raise IOError(detail)


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
        port = 50010
    return (host, port)

if __name__ == '__main__':
    pass

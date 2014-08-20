""" talk through a socket.socket """

import os
import sys
import time
import socket
import signal
import logging
import threading
from operator import attrgetter
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
        self.queue_condition = threading.Condition()
        self.ident = TalkIdent(idfile=IDFILE)
        self.talk_sockets = []
        self.talk_queues = []
        self.talk_listeners = []
        self.talk_actors = []
        # make log
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        # initialize value
        self.is_daemon = False
        self.stop = False
        self.queue_names = ['05_default']
        self.socket_addrs = ['127.0.0.1']
        self.actor_num = 2

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
        for queue_name in self.queue_names:
            self.add_queue(queue_name)
        for socket_addr in self.socket_addrs:
            talk_socket = self.add_socket(socket_addr)
            _add_thread(self._listen, args=(socket_addr,))
        for num in range(0, self.actor_num):
            _add_thread(self._act, args=(num,))
        # set signal handler
        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
        # wait thread
        while threading.active_count() > 2 and not self.stop:
            with self.condition:
                self.condition.wait(300)
        self.logger.info('-- listen end --')

    def _signal(self, signum, frame):
        """ for signal """
        sig_names = {23:"NSIG", 22:"SIGABRT", 21:"SIGBREAK", 8:"SIGFPE", 4:"SIGILL",
                     2:"SIGINT", 11:"SIGSEGV", 15:"SIGTERM", 0:"SIG_DFL", 1:"SIG_IGN"}
        self.logger.debug('received signal (%s)' % sig_names[signum])
        with self.condition:
            self.stop = True
            self.condition.notify()

    def add_queue(self, queue_name):
        """ add queue """
        talk_queue = TalkQueue(queue_name)
        self.talk_queues.append(talk_queue)
        sorted(self.talk_queues, key=attrgetter('name'))
        self.logger.info('"%s" queue start' % queue_name)
        return talk_queue

    def del_queue(self, queue_name):
        """ delete queue """
        with self.queue_condition:
            if _del_from_list(self.talk_queues, hash(queue_name)):
                self.logger.info('"%s" queue end' % queue_name)

    def add_socket(self, addr):
        """ add socket """
        try:
            talk_socket = TalkSocket(addr)
        except TalkError, detail:
            self.logger.error('"%s" socket failed to start because %s' % (addr,detail))
        else:
            self.talk_sockets.append(talk_socket)
            self.logger.info('"%s" socket start' % talk_socket.addr)

    def del_socket(self, addr):
        """ delete socket """
        (host, port) = _split_addr(addr)
        addr = ':'.join([host,str(port)])
        if _del_from_list(self.talk_sockets, hash(addr)):
            self.logger.info('"%s" socket end' % addr)

    def _listen(self, addr):
        """ start to listen the socket continuously """
        # listen thread info
        talk_listener = TalkListener(addr)
        listener_ident = talk_listener.ident
        addr = talk_listener.addr
        self.talk_listeners.append(talk_listener)
       # listen continuously
        while not _get_list_status(self.talk_listeners, listener_ident) == 'Stop':
            _set_list_status(self.talk_listeners, listener_ident, 'Listen')
            # get talk_socket
            for talk_socket in self.talk_sockets:
                if talk_socket.addr == addr:
                    break
            else:
                self.logger.error('can not find "%s" socket' % addr)
                break 
            # listen
            try:
                (conn,conn_addr) = talk_socket.listen()
            except TalkError, detail:
                self.logger.error('socket error @ "%s" because "%s"' % (addr,detail))
                break
            else:
                # enqueue on another thread
                self.logger.debug('"%s" is connected by "%s:%d"' % (addr,conn_addr[0],conn_addr[1]))
                _set_list_status(self.talk_listeners, listener_ident, 'Report')
                enqueue_thread = _add_thread(self._enqueue, args=(conn,conn_addr,))
        # end
        self.del_socket(addr)
        _del_from_list(self.talk_listeners, listener_ident)
        with self.condition:
            self.condition.notify()
        self.logger.info('listen end @ %s' % addr)

    def _enqueue(self, conn, conn_addr):
        """ enqueue to the queue """
        # receive a message
        is_error = False
        while True:
            try:
                message = conn.recv(4096) # bufsize
                message_list = message.split()
                self.logger.debug('from %s:%d get message "%s"' % (conn_addr[0],conn_addr[1],message))
                break
            except socket.error, detail:
                if not is_error:
                    self.logger.error('socket.error: %s' % detail)
                    is_error = True
                continue
        # check a message
        (reply_message, queue, queue_message) = self.check_message(message_list)
        # enqueue message
        if queue_message:
            with self.queue_condition:
                message_ident = self.ident.get()
                queue.put_item(message_ident, queue_message)
                # editable enqueue_act function
                reply_message = self.enqueue_act(queue.name, queue_message, message_ident)
                self.queue_condition.notify()
        # reply message
        conn.send(reply_message)
        conn.close()

    def _act(self, num):
        """ dequeue from queue and act """
        # act thread info
        talk_actor = TalkActor(num)
        actor_ident = talk_actor.ident
        actor_name = talk_actor.name
        self.talk_actors.append(talk_actor)
        self.logger.info('actor "%s" start' % actor_name)
        # get item from queue continuously
        while not _get_list_status(self.talk_actors, actor_ident) == 'Stop':
            _set_list_status(self.talk_actors, actor_ident, 'Wait')
            queue_message = None
            with self.queue_condition:
                while not queue_message:
                    # for all queue
                    for queue in self.talk_queues:
                        (queue_message, message_ident) = queue.get_item()
                        if queue_message:
                            self.logger.debug('actor "%s" get message "%s" from "%s"' % (actor_name,queue_message,queue.name))
                            break
                    else:
                        self.queue_condition.wait(60)
            # act
            _set_list_status(self.talk_actors, actor_ident, ('Act %d' % message_ident))
            self.dequeue_act(queue.name, queue_message, message_ident)
        # end
        with self.condition:
            self.condition.notify()
        _del_from_list(self.talk_actors, actor_ident)
        self.logger.info('act thread %d end' % num)

    def check_message(self, message_list):
        message_len = len(message_list)
        reply_message = ''
        queue = None
        queue_message = None
        if message_list[0] == 'socket':
            # socket info
            reply_message = 'Usage: socket [list]'
            if message_len > 1:
                if message_list[1] == 'list':
                    addr_list = []
                    for listener in self.talk_listeners:
                        addr_list.append('%s %s' % ('{0:<16}'.format(listener.addr),listener.status))
                    reply_message = '\n'.join(addr_list)
                    self.logger.info('send socket list')
        elif message_list[0] == 'queue':
            # manage queue
            reply_message = 'Usage: queue [list]'
            if message_len > 1:
                if message_list[1] == 'list':
                    name_list = []
                    for queue in self.talk_queues:
                        idents = ''
                        for ident in queue.idents:
                            if idents:
                                idents = ('%s %d' % (idents,ident))
                            else:
                                idents = ('%d' % ident)
                        name_list.append('%s %s (%s)' % ('{0:<16}'.format(queue.name),queue.status,idents))
                    reply_message = '\n'.join(name_list)
                    self.logger.info('send queue list')
        elif message_list[0] == 'actor':
            # manage actor
            reply_message = 'Usage: actor [list]'
            if message_len > 1:
                if message_list[1] == 'list':
                    name_list = []
                    for actor in self.talk_actors:
                        name_list.append('%s %s' % ('{0:<16}'.format(actor.name),actor.status))
                    reply_message = '\n'.join(name_list)
                    self.logger.info('send queue list')

        elif message_list[0] == 'enqueue':
            # enqueue check
            reply_message = 'Usage: enqueue <queue> <message>'
            if len(message_list) > 1:
                queue_name = message_list[1]
                for queue in self.talk_queues:
                    if queue_name == queue.name:
                        queue_message = ' '.join(message_list[2:])
                        break
                reply_message = ('cannot find "%s"' % queue_name)
        return (reply_message, queue, queue_message)

    def enqueue_act(self, queue_name, queue_message, message_ident=0):
        self.logger.info('enqueue to %s message [%d] "%s"' % (queue_name,message_ident,queue_message))
        reply_message = ('enqueue to "%s" number %d' % (queue_name,message_ident))
        return reply_message

    def dequeue_act(self, queue_name, queue_message, message_ident=0):
        self.logger.info('dequeue from %s message [%d] "%s"' % (queue_name, message_ident, queue_message))
        time.sleep(30)

class TalkError(Exception):
    pass

class TalkQueue:
    def __init__(self, name):
        self.name = name
        self.status = 'Start'
        self.ident = hash(name)
        self.condition = threading.Condition()
        self.queue = []
        self.idents = []

    def put_item(self, ident, item):
        with self.condition:
            self.idents.append(ident)
            self.queue.append(str(item))
        self.status = 'Remain'

    def get_item(self):
        ident = None
        item = None
        with self.condition:
            if len(self.queue) > 0:
                ident = self.idents.pop(0)
                item = self.queue.pop(0)
            else:
                self.status = 'Empty'
        return (item, ident)

    def list_item(self):
        item_list = []
        for ident,item in zip(self.idents,self.queue):
            item_list.append('%s %s' %(str(ident).zfill(3),item))
        return item_list 

class TalkSocket:
    def __init__(self, addr):
        (host, port) = _split_addr(addr)
        addr = ':'.join([host,str(port)])
        self.addr = addr
        self.ident = hash(addr)
        self.status = 'Start'
        self.condition = threading.Condition()
        # add socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(30)
        try:
            self.socket.bind((host, int(port)))
        except socket.error, (errno,detail): 
            raise TalkError(detail)

    def listen(self):
        conn = None
        conn_addr = None
        with self.condition:
            while not conn:
                try:
                    self.socket.listen(1) # backlog
                    (conn, conn_addr) = self.socket.accept()
                    # wait to be connected
                except socket.timeout:
                    continue
                except socket.error, (errno, detail):
                    if not errno == 9: # EBADF
                        self.socket.close()
                    raise TalkError(detail)
        return (conn, conn_addr)

actor_names = ('Appleton', 'Bohr', 'Curie', 'Dirac', 'Einstein', 'Fermi', 'Guillaume', 'Hertz', 'Ioffe', 'Jensen',
               'Koshiba', 'Lorentz', 'Michelson', 'Neel' ,'Onnes', 'Planck')

class TalkActor:
    def __init__(self, num):
        self.ident = threading.currentThread().ident
        self.status = 'Start'
        self.num = num
        self.name = actor_names[num]

class TalkListener:
    def __init__(self, addr):
        self.ident = threading.currentThread().ident
        self.status = 'Start'
        (host, port) = _split_addr(addr)
        addr = ':'.join([host,str(port)])
        self.addr = addr

class TalkIdent():
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

def _del_from_list(list, ident):
    for num,target in enumerate(list):
        if target.ident == ident:
            del list[num]
            return True
        return False
 
def _set_list_status(list, ident, status):
    for num,target in enumerate(list):
        if target.ident == ident:
            list[num].status = status
            return True
    return False

def _get_list_status(list, ident):
    for num,target in enumerate(list):
        if target.ident == ident:
            return list[num].status
    return None

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

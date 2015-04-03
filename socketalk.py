""" talk through a socket """

import socket
import signal
from threading import Condition, Thread
from logging import getLogger, FileHandler, Formatter, DEBUG
from talkdata import TalkDB, SocketData, MessageData


class SocketTalk(object):
    """
    listen and speak through a socket,
    check messages and respond,
    """
    buffer_size = 4096
    default_port = 50010
    log_format = '%(asctime)-15s %(filename)s %(levelname)s %(message)s'
    sig_names = {23: 'NSIG',
                 22: 'SIGABRT',
                 21: 'SIGBREAK',
                 8: 'SIGFPE',
                 4: 'SIGILL',
                 2: 'SIGINT',
                 11: 'SIGSEGV',
                 15: 'SIGTERM',
                 0: 'SIG_DFL',
                 1: 'SIG_IGN'}

    def __init__(self, db_url='sqlite://', logfile='/dev/stdout'):
        self.status = 'Init'
        # make log
        self.logger = getLogger()
        self.logger.setLevel(DEBUG)
        self.handler = FileHandler(logfile)
        self.handler.level = DEBUG
        self.handler.formatter = Formatter(self.log_format)
        self.logger.addHandler(self.handler)
        # make database and queue
        self.talk_db = TalkDB(db_url)
        # make thread condition
        self.condition = Condition()
        self.list_condition = Condition()
        self.sockets = {}

    def speak(self, addr, message, timeout=5):
        """ speak through a socket """
        if len(message) > self.buffer_size:
            raise TalkError('message is too large')
        # send message
        (host, port) = self.split_addr(addr)
        speak_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        speak_socket.settimeout(timeout)
        self.logger.info('speak to %s:%d' % (host, port))
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

    def listen(self):
        # make info
        self.message_id = 0
        self.origins = {}
        # set signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # start
        self.logger.info('-- listen start --')
        self.status = 'Start'
        self.talk_db.commit()
        # start threads
        for sock in self.talk_db.get(SocketData):
            self.start_socket(sock.addr)
        # wait
        while (len(self.sockets) > 0) and (self.status != 'Stop'):
            self.main_loop()
        # del threads
        for sock in self.talk_db.get(SocketData):
            self.stop_socket(sock.addr)
        self.logger.info('-- listen end --')

    def signal_handler(self, signum, frame):
        self.logger.info('received signal(%s)' % self.sig_names[signum])
        self.status = 'Stop'
        with self.condition:
            self.condition.notify()

    def main_loop(self):
        with self.condition:
            self.condition.wait(300)

    def add_socket(self, addr):
        """ enter a socket data to the database """
        (host, port) = self.split_addr(addr)
        addr = ':'.join([host, str(port)])
        for sock in self.talk_db.get(SocketData):
            if sock.addr == addr:
                self.logger.debug('[%s] socket already exists' % addr)
                break
        else:
            new_data = SocketData(addr)
            self.talk_db.add(new_data)
            self.logger.info('add the [%s] socket' % addr)

    def del_socket(self, addr):
        """ delete the socket data from the database """
        (host, port) = self.split_addr(addr)
        addr = ':'.join([host, str(port)])
        for sock in self.session.query(SocketData):
            if sock.addr == addr:
                self.talk_db.delete(SocketData, sock)
                self.logger.info('delete the "%s" socket' % addr)
        else:
            self.logger.info('[%s] socket does not exists' % addr)

    def start_socket(self, addr):
        """ start a socket thread """
        (host, port) = self.split_addr(addr)
        addr = ':'.join([host, str(port)])
        talk_socket = TalkSocket(addr)
        with self.list_condition:
            self.sockets[addr] = talk_socket
        try:
            add_thread(self.listen_socket, args=(addr, ))
            return True
        except TalkError, detail:
            self.logger.warn('[%s] socket is failed to start because %s'
                             % (addr, detail))
            return False

    def stop_socket(self, addr):
        """ stop the socket thread """
        (host, port) = self.split_addr(addr)
        addr = ':'.join([host, str(port)])
        if addr in self.sockets:
            self.sockets[addr].status = 'Stop'
        else:
            self.logger.warn('cannot find the [%s] socket' % addr)
            return False
        with self.condition:
            # inform the socket
            tmp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmp_socket.connect((host, int(port)))
            self.condition.wait(9)
        with self.list_condition:
            del self.sockets[addr]
        return True

    def listen_socket(self, addr):
        """ listen the socket and spawn a checker theread """
        if addr not in self.sockets:
            self.logger.error('cannot find the socket "%s"' % addr)
            return
        self.sockets[addr].status = 'Start'
        self.logger.info('socket start @ %s' % addr)
        # listen continuously
        while self.sockets[addr].status != 'Stop':
            # listen
            try:
                (conn, conn_addr) = self.sockets[addr].open()
                self.logger.info('[%s] is connected by [%s:%d]'
                                 % (addr, conn_addr[0], conn_addr[1]))
                # receive a message and spawn a thread
                add_thread(self.receive_message, args=(conn, conn_addr, ))
            except TalkError, detail:
                self.logger.error('socket error @ "%s" because "%s"'
                                  % (addr, detail))
                break
        # end
        self.sockets[addr].close()
        with self.condition:
            self.condition.notifyAll()
        self.logger.info('socket end @ %s' % addr)

    def receive_message(self, conn, conn_addr):
        """ receive a message and reply """
        # receive a message
        message = None
        for num in range(10):
            try:
                message = conn.recv(self.buffer_size)
                break
            except socket.error:
                continue
        if message:
            origin = ('%s:%d' % (conn_addr[0], conn_addr[1]))
            with self.condition:
                self.message_id += 1
                message_id = self.message_id
            self.origins[message_id] = origin
            self.logger.debug('get message[%d] from %s' % (message_id, origin))
            # reply message
            reply = self.check_received(message, message_id)
            conn.send(reply)
            self.logger.debug('reply message[%d] to %s' % (message_id, origin))
        conn.close()

    def check_received(self, message, message_id):
        """ check instruction """
        reply = ('%d %s' % (message_id, message))
        return reply

    def get_sockets(self):
        list = []
        for addr in self.sockets.keys():
            list.append('%s %s' % (addr, self.sockets[addr].status))
        self.logger.debug('get lister list')
        return ('%s' % '\n'.join(list))

    def get_origin(self, message_id):
        return self.origins[message_id]

    def split_addr(self, addr):
        """ get host and port from addr """
        addr_list = str(addr).split(':')
        host = addr_list[0]
        if len(addr_list) > 1:
            port = int(addr_list[1])
        else:
            port = self.default_port
        return (host, port)


class StoreTalk(SocketTalk):
    """
    treat received message by a queue
    entry messages to a databese.
    """

    def __init__(self, db_url='sqlite://', logfile='/dev/stdout'):
        super(StoreTalk, self).__init__(db_url, logfile)
        self.talk_queue = TalkQueue()

    def check_received(self, message, message_id):
        """ check instruction """
        self.enqueue_message(message, message_id)
        reply = ('%d' % message_id)
        return reply

    def enqueue_message(self, message, message_id):
        """ enqueue message """
        with self.condition:
            self.talk_queue.push(message, message_id)
            self.condition.notifyAll()
        self.logger.debug('enqueue message[%d]' % message_id)

    def main_loop(self):
        (message, message_id) = self.dequeue_message()
        if message:
            self.talk_db.commit()
            self.check_dequeued(message, message_id)
        else:
            with self.condition:
                self.condition.wait(300)

    def dequeue_message(self):
        """ dequeue from a queue and work """
        # get item from queue continuously
        (message, message_id) = self.talk_queue.pop()
        if message:
            self.logger.info('dequeue message[%d]' % message_id)
        return (message, message_id)

    def check_dequeued(self, message, message_id):
        self.store_message(message, message_id)

    def store_message(self, message, message_id):
        """ put message in database """
        origin = self.get_origin(message_id)
        new_data = MessageData(message, origin)
        self.talk_db.add(new_data)
        del self.origins[message_id]


class TalkError(Exception):
    pass


class TalkSocket:
    """ make socket and listen """

    def __init__(self, addr, timeout=300):
        (self.host, self.port) = str(addr).split(':')
        self.addr = addr
        self.condition = Condition()
        # add socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.settimeout(timeout)
        try:
            self.socket
            self.socket.bind((self.host, int(self.port)))
        except socket.error, (errno, detail):
            raise TalkError('socket.error: %s' % detail)
        self.status = 'Init'

    def open(self):
        conn = None
        conn_addr = None
        self.status = 'Listen'
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
        if self.status != 'Stop':
            self.status = ('Connected by %s:%d' % (conn_addr[0], conn_addr[1]))
        return (conn, conn_addr)

    def close(self):
        self.socket.close()


class TalkQueue:
    """ queue definition """

    def __init__(self):
        self.condition = Condition()
        self.idents = []
        self.items = []
        self.status = 'Empty'

    def push(self, item, ident):
        with self.condition:
            self.idents.append(ident)
            self.items.append(str(item))
            self.status = ('Remain %s' % len(self.items))

    def pop(self):
        with self.condition:
            if len(self.items) > 0:
                ident = self.idents.pop(0)
                item = self.items.pop(0)
            else:
                ident = None
                item = None
                self.status = 'Empty'
        return (item, ident)

    def list(self):
        list = []
        with self.condition:
            for ii in range(len(self.idents)):
                list.append('%s %s' % (self.idents[ii], self.items[ii]))
        return ('%s' % '\n'.join(list))


def add_thread(target, args=()):
    """ start a thread """
    new_thread = Thread(target=target, args=args)
    new_thread.setDaemon(True)
    new_thread.start()
    return new_thread


if __name__ == '__main__':
    pass


import socket
from signal import signal, SIGINT, SIGTERM
from threading import Condition, Thread
from logging import getLogger, FileHandler, Formatter, DEBUG, INFO, WARN
from datautil import TalkDB, MessageData
from Queue import Queue, Empty

BUFFER_SIZE = 4096
DEFAULT_PORT = 50010
LOG_FORMAT = '%(asctime)-15s %(filename)s %(levelname)s %(message)s'
SIG_NAMES = {23: 'NSIG',
             22: 'SIGABRT',
             21: 'SIGBREAK',
             8: 'SIGFPE',
             4: 'SIGILL',
             2: 'SIGINT',
             11: 'SIGSEGV',
             15: 'SIGTERM',
             0: 'SIG_DFL',
             1: 'SIG_IGN'}


class SocketTalk(object):
    """
    listen and speak through a socket,
    check messages and respond,
    """

    def __init__(self, logfile='/dev/stdout', loglevel='debug'):
        self.status = 'Init'
        # make log
        self.logger = getLogger()
        self.logger.setLevel(DEBUG)
        handler = FileHandler(logfile)
        if loglevel.lower() == 'debug':
            handler.level = DEBUG
        elif loglevel.lower() == 'warn':
            handler.level = WARN
        else:
            handler.level = INFO
        handler.formatter = Formatter(LOG_FORMAT)
        self.logger.addHandler(handler)
        # make thread condition
        self.condition = Condition()
        # make other info
        self.list_condition = Condition()
        self.message_id = 0
        self.sockets = {}
        self.connects = {}

    def speak(self, addr, message, timeout=10):
        """ speak through a socket """
        if len(message) > BUFFER_SIZE:
            raise TalkError('speak message is too large')
        if not self.check_speaking(message):
            raise TalkError('speak message is incorrect')
        # send message
        (host, port, addr) = split_addr(addr)
        speak_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        speak_socket.settimeout(timeout)
        self.logger.info('speak to %s:%d', host, port)
        try:
            speak_socket.connect((host, port))
            speak_socket.send(message)
            reply = speak_socket.recv(BUFFER_SIZE)
        except socket.error, detail:
            speak_socket.close()
            raise TalkError('socket.error: %s' % detail)
        except socket.timeout, detail:
            speak_socket.close()
            raise TalkError('socket.timeout: %s' % detail)
        else:
            # accept message
            speak_socket.close()
            return reply

    def check_speaking(self, message):
        """ check speaking message and deside speak or not """
        if message:
            return True
        else:
            return False

    def listen(self):
        """ listen main """
        self.listen_init()
        # start
        self.logger.info('-- listen start --')
        self.status = 'Start'
        # start threads
        for sock in self.sockets:
            self.start_socket(sock)
        # wait
        while self.status != 'Stop':
            self.main_loop()
        # del threads
        for sock in self.sockets:
            self.stop_socket(sock)
        self.listen_finalize()
        self.logger.info('-- listen end --')

    def listen_init(self):
        # set signal handler
        signal(SIGINT, self.signal_handler)
        signal(SIGTERM, self.signal_handler)

    def listen_finalize(self):
        pass

    def signal_handler(self, signum, frame):
        """ handle signal to stop """
        self.logger.info('received signal(%s)', SIG_NAMES[signum])
        self.status = 'Stop'
        with self.condition:
            self.condition.notify()

    def main_loop(self):
        """ main thread loop """
        with self.condition:
            self.condition.wait(300)

    def add_socket(self, addr):
        """ enter a socket data to the database """
        addr = split_addr(addr)[2]
        for sock in self.sockets:
            if sock == addr:
                self.logger.debug('cannot add the [%s] socket: already exists',
                                  addr)
                break
        else:
            talk_socket = TalkSocket(addr)
            with self.list_condition:
                self.sockets[addr] = talk_socket
            self.logger.info('add the [%s] socket', addr)

    def del_socket(self, addr):
        """ delete the socket data from the database """
        addr = split_addr(addr)[2]
        if addr in self.sockets:
            with self.list_condition:
                self.stop_socket(addr)
                del self.sockets[addr]
                self.logger.info('delete the [%s] socket', addr)
        else:
            self.logger.debug('cannot delete the [%s] socket: not found', addr)

    def get_sockets(self):
        """ get socket names and status"""
        socket_list = []
        for addr in self.sockets.keys():
            socket_list.append('%s %s' % (addr, self.sockets[addr].status))
        self.logger.debug('get socket list')
        return '%s' % '\n'.join(socket_list)

    def start_socket(self, addr):
        """ start a socket thread """
        addr = split_addr(addr)[2]
        if addr not in self.sockets:
            self.logger.warn('cannot start the [%s] socket: not found', addr)
            return False
        try:
            add_thread(self.listen_socket, args=(addr, ))
            return True
        except TalkError, detail:
            self.logger.warn('cannot start the [%s] socket: %s',
                             addr, detail)
            return False

    def stop_socket(self, addr):
        """ stop the socket thread """
        (host, port, addr) = split_addr(addr)
        if addr not in self.sockets:
            self.logger.warn('cannot stop the [%s] socket: not found', addr)
            return False
        self.sockets[addr].status = 'Stop'
        with self.condition:
            # inform the socket
            tmp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tmp_socket.connect((host, int(port)))
            self.condition.wait(9)
        return True

    def listen_socket(self, addr):
        """ listen the socket and spawn a checker theread """
        self.sockets[addr].status = 'Start'
        self.logger.info('socket start @ %s', addr)
        # listen continuously
        while self.sockets[addr].status != 'Stop':
            try:
                (connect, connect_addr) = self.sockets[addr].open()
                # receive a message and spawn a thread
                origin = '%s:%d' % (connect_addr[0], connect_addr[1])
                self.logger.info('[%s] is connected by [%s]',
                                 addr, origin)
                add_thread(self.receive_message, args=(connect, origin))
            except TalkError, detail:
                self.logger.error('[%s] socket error: %s', addr, detail)
                break
        # end
        self.sockets[addr].close()
        with self.condition:
            self.condition.notifyAll()
        self.logger.info('socket end @ %s', addr)

    def receive_message(self, connect, origin):
        """ receive a message and reply """
        # receive a message
        message = None
        for count in range(10):
            try:
                message = connect.recv(BUFFER_SIZE)
                break
            except socket.error:
                continue
        if message:
            self.connects[origin] = connect
            self.logger.debug('get message [%s]', origin)
            # reply message
            self.check_received(message, origin)

    def check_received(self, message, origin):
        """ check and reply """
        self.connects[origin].send(origin)
        self.logger.debug('reply message [%s]' % origin)
        self.connects[origin].close()
        del self.connects[origin]


class StoreTalks(SocketTalk):
    """
    treat received message by a queue
    entry messages to a databese.
    """

    def __init__(self, logfile='/dev/stdout', loglevel='debug',
                 db_url='sqlite://'):
        super(StoreTalks, self).__init__(logfile, loglevel)
        self.talk_queue = Queue()
        self.talk_db = TalkDB(db_url)

    def listen_finalize(self):
        super(StoreTalks, self).listen_finalize()
        # self.talk_queue.join()

    def check_received(self, message, origin):
        """ enqueue """
        self.enqueue_message(message, origin)

    def enqueue_message(self, message, origin):
        with self.condition:
            self.talk_queue.put((message, origin))
            self.condition.notifyAll()
        self.logger.debug('enqueue message [%s]', origin)

    def dequeue_message(self):
        try:
            (message, origin) = self.talk_queue.get_nowait()
            self.talk_queue.task_done()
            self.logger.info('dequeue message [%s]', origin)
            return (message, origin)
        except Empty:
            return (None, None)

    def main_loop(self):
        """ main thread dequeue and store message """
        (message, origin) = self.dequeue_message()
        if message:
            self.check_dequeued(message, origin)
        else:
            with self.condition:
                self.condition.wait(300)

    def check_dequeued(self, message, origin):
        """ check dequeued message """
        self.store_message(message, origin)
        self.connects[origin].send('messsage stored')
        self.logger.debug('reply message [%s]' % origin)
        self.connects[origin].close()

    def store_message(self, message, origin):
        """ put message in database """
        new_data = MessageData(message, origin)
        self.talk_db.add(new_data)
        self.talk_db.commit()
        self.logger.debug('store message [%s]' % origin)


class TalkError(Exception):
    """ Talk Error class """
    pass


class TalkSocket(object):
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
            self.socket.bind((self.host, int(self.port)))
        except socket.error, (errno, detail):
            raise TalkError('socket.error: [%d] %s' % (errno, detail))
        self.status = 'Init'

    def open(self):
        """ socket listen and accept """
        connect = None
        connect_addr = None
        self.status = 'Listen'
        with self.condition:
            while not connect:
                try:
                    self.socket.listen(1)  # backlog
                    (connect, connect_addr) = self.socket.accept()
                    # wait to be connected
                except socket.timeout:
                    continue
                except socket.error, (errno, detail):
                    if not errno == 9:  # EBADF
                        self.socket.close()
                        raise TalkError('socket.error: %s' % detail)
        if self.status != 'Stop':
            self.status = 'Connected'
        return (connect, connect_addr)

    def close(self):
        """ close socket """
        self.socket.close()


def add_thread(target, args=()):
    """ start a thread """
    new_thread = Thread(target=target, args=args)
    new_thread.setDaemon(True)
    new_thread.start()
    return new_thread


def split_addr(addr):
    """ get host and port from addr """
    addr_list = str(addr).split(':')
    host = addr_list[0]
    if len(addr_list) > 1:
        port = int(addr_list[1])
    else:
        port = DEFAULT_PORT
    addr = ':'.join([host, str(port)])
    return (host, port, addr)


if __name__ == '__main__':
    pass

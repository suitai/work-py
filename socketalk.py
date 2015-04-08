
import os
import sys
import getopt
import logging
import threading
import SocketServer
import Queue

import daemon
import lockfile.pidlockfile

from sqlalchemy.schema import Column
from sqlalchemy.types import Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

BUFFER_SIZE = 1024
DEFAULT_PORT = 50010


class ThreadedServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


class ServerComplex(object):

    def __init__(self, handler):
        self.handler = handler
        self.servers = {}
        self.status = 'Init'
        self.condition = threading.Condition()

    def add(self, host, port):
        """ enter a socket data to the database """
        addr = '%s:%d' % (host, port)
        if addr not in self.servers:
            server = ThreadedServer((host, port), self.handler)
            with self.condition:
                self.servers[addr] = server
            if self.status == 'Start':
                self.start(server)
        return server

    def delete(self, host, port):
        """ delete the socket data from the database """
        addr = '%s:%d' % (host, port)
        if addr in self.servers:
            if self.status == 'Start':
                self.stop(self.servers[addr])
            with self.condition:
                del self.servers[addr]

    def start(self, server):
        """ start a server thread """
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.setDaemon(True)
        server_thread.start()

    def stop(self, server):
        """ stop a server thread """
        server.shutdown()

    def start_all(self):
        """ start server threads """
        for server in self.servers.itervalues():
            self.start(server)
        self.status = 'Start'

    def stop_all(self):
        """ stop a server thread """
        for server in self.servers.itervalues():
            self.stop(server)
        self.status = 'Stop'

    def get_socket_list(self):
        socket_list = []
        for addr in self.servers:
            socket_list.append(addr)
        return socket_list


class MessageData(Base):
    __tablename__ = 'message'
    id = Column(Integer, primary_key=True)
    text = Column(String)
    origin = Column(String)

    def __init__(self, text, origin=None):
        self.text = text
        self.origin = origin

    def __repr__(self):
        return('WorkerData(%d, %s, %s, %s)'
               % (self.id, self.text, self.origin))


class Command(object):
    """
    start or stop a listen daemon
    """

    option = ''
    long_option = []

    def __init__(self, argv):
        self.name = argv[0]
        self.argv = argv[1:]
        self.operates = {}

    def get_opts(self):
        """ check argv """
        # handle options
        try:
            (opts, args) = getopt.getopt(self.argv, self.option,
                                         self.long_option)
        except getopt.GetoptError, detail:
            raise Exception('GetoptError: %s' % detail)
        return (opts, args)

    def check_opts(self, opts, args):
        return ''

    def usage(self):
        sys.stdout.write('Usage')

    def main(self):
        (opts, args) = self.get_opts()
        operate = self.check_opts(opts, args)
        self.operates.get(operate, self.usage)()


def get_logger(logfile, loglevel):
    log_format = '%(asctime)-15s %(filename)s %(levelname)s %(message)s'
    loglevels = {'debug': logging.DEBUG,
                 'warn': logging.WARN,
                 'info': logging.INFO}
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(logfile)
    handler.level = loglevels.get(loglevel, logging.INFO)
    handler.formatter = logging.Formatter(log_format)
    logger.addHandler(handler)
    return logger


def daemonize(function, pidfile, logfile):
    lock = lockfile.pidlockfile.PIDLockFile(pidfile)
    if lock.is_locked():
        raise Exception('The daemon already exists')
    file = open(logfile, 'a')
    context = daemon.DaemonContext(pidfile=lock, stdout=file, stderr=file)
    with context:
        function()


def get_spaces(num):
    space = ''
    for count in range(num):
        space += ' '
    return space


def get_pid_from_file(pidfile):
    """ get pid from pidfile """
    if os.path.exists(pidfile):
        with open(pidfile, 'r') as file:
            return int(file.read())
    else:
        sys.exit('Info: Worker is not running')


if __name__ == '__main__':
    pass

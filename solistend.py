#!/usr/bin/env python
""" command to start a socket listen daemon"""

import os
import sys
import time
import signal
import socket
import threading
import subprocess
import SocketServer

try:
    import socketalk
except ImportError, detail:
    sys.exit('ImportError: %s' % detail)


class SocketListen(object):
    """
    listen and speak through a socket,
    check messages and respond,
    """

    sig_names = {2: 'SIGINT',
                 15: 'SIGTERM'}

    def __init__(self, logfile='/dev/stdout', loglevel='debug'):
        self.logger = socketalk.get_logger(logfile, loglevel)
        self.server_cmplx = None
        self.condition = threading.Condition()
        self.status = 'Init'

    def listen_setup(self):
        logger = self.logger

        class SimpleResponse(SocketServer.BaseRequestHandler):
            def handle(self):
                message = self.request.recv(socketalk.BUFFER_SIZE)
                thread_name = threading.currentThread().getName()
                address = self.client_address
                logger.info('%s get message from %s:%s'
                            % (thread_name, address[0], address[1]))
                response = '%s: %s' % (thread_name, message)
                self.request.send(response)

        self.server_cmplx = socketalk.ServerComplex(SimpleResponse)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def listen(self):
        """ listen main """
        self.logger.info('-- listen start --')
        self.status = 'Start'
        self.server_cmplx.start_all()
        while self.status != 'Stop':
            with self.condition:
                self.condition.wait(300)
        self.server_cmplx.stop_all()
        self.logger.info('-- listen end --')

    def signal_handler(self, signum, frame):
        """ handle signal to stop """
        self.logger.info('received signal(%s)', self.sig_names[signum])
        self.status = 'Stop'
        with self.condition:
            self.condition.notify()

    def add_socket(self, host, port):
        """ add a socket server """
        self.server_cmplx.add(host, port)
        self.logger.debug('add the socket %s:%d' % (host, int(port)))

    def del_socket(self, host, port):
        """ delete a socket server """
        self.server_cmplx.delete(host, port)
        self.logger.debug('delete the socket %s:%d' % (host, int(port)))

    def get_sockets(self):
        """ get socket names and status"""
        socket_list = self.server_cmplx.get_socket_list()
        self.logger.debug('get socket list')
        return '%s' % '\n'.join(socket_list)


class ListenCommand(socketalk.Command):
    """
    start or stop a listen daemon
    """

    option = 'dp:'
    long_option = ['help', 'kill', 'log']
    pidfile = '/var/run/listend.pid'
    logfile = '/var/log/listend.log'
    hosts = ['127.0.0.1']
    port = socketalk.DEFAULT_PORT

    def __init__(self, argv):
        super(ListenCommand, self).__init__(argv)
        self.is_daemon = False
        self.operates = {'usage': self.usage,
                         'run': self.run,
                         'kill': self.kill,
                         'log': self.log}

    def check_opts(self, opts, args):
        # check longoption
        if len(opts) == 1:
            if opts[0][0] == '--kill':
                return 'kill'
            elif opts[0][0] == '--log':
                return 'log'
        # check option
        for (opt, arg) in opts:
            if opt == '-p':
                self.port = int(arg)
            elif opt == '-d':
                self.is_daemon = True
            else:
                return 'usage'
        return 'run'

    def usage(self):
        """ print usage """
        space = socketalk.get_spaces(len(self.name))
        sys.exit('Usage: %s [--help|--kill]\n'
                 '       %s [-p port] [-d]' % (self.name, space))

    def listen(self):
        """ listen main """
        self.hosts.append(socket.gethostbyname(socket.gethostname()))
        listen = SocketListen()
        listen.listen_setup()
        for host in self.hosts:
            listen.add_socket(host, self.port)
        try:
            listen.listen()
        except Exception, detail:
            sys.exit('Error: %s' % detail)

    def run(self):
        """ start process """
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        if self.is_daemon:
            socketalk.daemonize(self.listen, self.pidfile, self.logfile)
        else:
            self.listen()

    def kill(self):
        """ stop daemon """
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        pid = socketalk.get_pid_from_file(self.pidfile)
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError, detail:
            sys.exit('Error: Cannot kill Worker: %s' % detail)
        while os.path.exists(self.pidfile):
            time.sleep(1)

    def log(self):
        cmd = ['/usr/bin/less', self.logfile]
        pid = subprocess.Popen(cmd)

        def send_signal(signum, frame):
            pid.send_signal(signal.SIGINT)

        signal.signal(signal.SIGINT, send_signal)
        pid.communicate()


if __name__ == '__main__':
    ListenCommand(sys.argv).main()

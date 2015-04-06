#!/usr/bin/env python
""" command to start socket listen daemon"""

import os
import sys
import time
import subprocess
import signal
from signal import SIGTERM
from daemon import DaemonContext
from lockfile.pidlockfile import PIDLockFile
from getopt import getopt, GetoptError
from socket import gethostname, gethostbyname

try:
    from socketalk import StoreTalks, TalkError
except ImportError, detail:
    sys.exit('ImportError: %s' % detail)


class ListenCommand(object):
    """
    start, stop, and show statu of the listen daemon
    """

    option = 'dp:'
    long_option = ['help', 'kill', 'log']
    pidfile = '/var/run/listend.pid'
    logfile = '/var/log/listend.log'
    hosts = ['127.0.0.1']
    port = 50010
    database = 'postgresql://kmm:postgres@localhost:5432/testdb'

    def __init__(self, argv):
        self.name = argv[0]
        self.argv = argv[1:]
        self.is_daemon = False

    def check_argv(self):
        """ check argv """
        # handle options
        try:
            (opt_list, other_args) = getopt(self.argv, self.option,
                                            self.long_option)
        except GetoptError, detail:
            sys.exit('GetoptError: %s' % detail)
        # check longoption
        if len(opt_list) == 1:
            if opt_list[0][0] == '--kill':
                return 'kill'
            elif opt_list[0][0] == '--log':
                return 'log'
        # check option
        for (opt, arg) in opt_list:
            if opt == '-p':
                self.port = int(arg)
            elif opt == '-d':
                self.is_daemon = True
            else:
                return 'usage'
        return 'run'

    def usage(self):
        """ print usage """
        space = get_space(len(self.name))
        sys.exit('Usage: %s [--help|--kill]\n'
                 '       %s [-p port] [-d]' % (self.name, space))

    def listen(self):
        """ listen main """
        self.hosts.append(gethostbyname(gethostname()))
        talk = StoreTalks(db_url=self.database)
        for host in self.hosts:
            addr = '%s:%d' % (host, self.port)
            talk.add_socket(addr)
        try:
            talk.listen()
        except TalkError, detail:
            sys.exit('Error: %s' % detail)

    def run(self):
        """ start process """
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        if self.is_daemon:
            daemonize(self.listen, self.pidfile, self.logfile)
        else:
            self.listen()

    def kill(self):
        """ stop daemon """
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        pid = get_pid_from_file(self.pidfile)
        try:
            os.kill(pid, SIGTERM)
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

    def main(self):
        operate = self.check_argv()
        if operate == 'run':
            self.run()
        elif operate == 'kill':
            self.kill()
        elif operate == 'log':
            self.log()
        else:
            self.usage()


def daemonize(func, pidfile, logfile):
    lock = PIDLockFile(pidfile)
    if lock.is_locked():
        sys.exit('cannot start daemon: %s already exists' % lock.lock_file)
    context = DaemonContext(pidfile=lock,
                            stdout=open(logfile, 'a'))
    with context:
        func()


def get_space(num):
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
    LISTEN = ListenCommand(sys.argv)
    LISTEN.main()

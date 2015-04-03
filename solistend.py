#!/usr/bin/env python
""" command to start socket listen daemon"""

import os
import sys
import time
from signal import SIGTERM

try:
    import psutil
    from daemon import DaemonContext
    from lockfile.pidlockfile import PIDLockFile
    from socketalk import StoreTalk, TalkError
except ImportError, detail:
    sys.exit('ImportError: %s' % detail)


class SocketListen(StoreTalk):
    """
    daemonize listen process
    """

    def __init__(self, db_url='sqlite://', logfile='/dev/stdout'):
        super(SocketListen, self).__init__(db_url, logfile)

    def listen_daemon(self, pidfile):
        """ deamone process start """
        # check lockfile
        lock = PIDLockFile(pidfile)
        if lock.is_locked():
            raise TalkError('%s already exists' % lock.lock_file)
        # daemonize
        self.status = 'Daemonize'
        context = DaemonContext(pidfile=lock,
                                files_preserve=[self.handler.stream])
        with context:
            self.listen()


class ListenCommand:
    """
    start, stop, and show statu of the listen daemon
    """

    pidfile = '/var/run/listend.pid'
    logfile = '/var/log/listend.log'
    addr = '127.0.0.1:50010'
    database = 'postgresql://kmm:postgres@localhost:5432/testdb'

    def __init__(self):
        pass

    def usage(self):
        sys.stdout.write('Usage: listend [start|stop|status]\n')

    def start(self):
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        t = SocketListen(self.database, self.logfile)
        t.add_socket(self.addr)
        t.listen_daemon(pidfile=self.pidfile)

    def stop(self):
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        pid = get_pid_from_file(self.pidfile)
        try:
            os.kill(pid, SIGTERM)
        except OSError, detail:
            sys.exit('Error: Cannot kill Worker: %s' % detail)
        while os.path.exists(self.pidfile):
            time.sleep(1)

    def restart(self):
        self.stop()
        self.start()

    def status(self):
        pid = get_pid_from_file(self.pidfile)
        if pid in psutil.get_pid_list():
            sys.stdout.write('Info: listend is running\n')
        else:
            sys.exit('Error: listend is not running')

    def run(self):
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        t = SocketListen(self.database)
        t.add_socket(self.addr)
        t.listen()


def get_pid_from_file(pidfile):
    if os.path.exists(pidfile):
        with open(pidfile, 'r') as f:
            return int(f.read())
    else:
        sys.exit('Info: Worker is not running')


if __name__ == "__main__":
    l = ListenCommand()
    if len(sys.argv) > 1:
        operate = {'start': l.start,
                   'stop': l.stop,
                   'restart': l.restart,
                   'status': l.status,
                   'run': l.run}
        if sys.argv[1] in operate.keys():
            operate.get(sys.argv[1], l.usage)()
    else:
        l.usage()

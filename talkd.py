#!/usr/bin/env python
""" talk through a socket.socket """

import os
import sys
from getopt import getopt, GetoptError
from signal import SIGTERM
from daemon import DaemonContext
from lockfile.pidlockfile import PIDLockFile

import talk
from talk import Listener

SELF_HOST = '127.0.0.1:50010'
PID_FILE = '/var/run/talk.pid'
LOG_FILE = 'talk.log'

class ListenerDaemon(Listener):
    """ listen through a socket.socket """
    def __init__(self, more_host=None, pidfile=PID_FILE, logfile=LOG_FILE):
        Listener.__init__(self, more_host)
        self.pidfile = pidfile
        self.logfile = logfile

    def daemonize(self):
        """ deamone process """
        try:
            fileout = open(self.logfile, 'a')
        except IOError, detail:
            sys.exit('IOError: %s' % detail)
        filelock = PIDLockFile(self.pidfile)
        # check lockfile
        if filelock.is_locked():
            sys.exit('%s already exists, exitting' % filelock.lock_file)
        # daemonize
        context = DaemonContext(pidfile=filelock, stdout=fileout, stderr=fileout)
        for host in self.host_list:
            sys.stdout.write('talk listen start @ %s \n' % host)    
        with context:
            self.main()

    def kill(self):
        """ kill the daemon process"""
        # get pid
        try:
            pidfile = open(self.pidfile, 'r')
        except IOError, detail:
            sys.exit('IOError: %s' % detail)
        pid = int(pidfile.read())
        # kill
        try:
            os.kill(pid, SIGTERM)
        except OSError, detail:
            sys.exit('OSError: %s' % detail)

def usage():
    print 'usage'

if __name__ == "__main__":
    """ command execution """
    opt_host = None
    try:
        action = sys.argv[1]
    except IndexError:
        usage()
    # handle options
    option = "h"
    long_option = ["help", "add="] 
    try:
        opt_list, odd_arg_list = getopt(sys.argv[2:], option, long_option)
    except GetoptError, detail:
        sys.exit('GetoptError: %s' % detail)
    for opt, arg in opt_list:
        if opt in ('-h', '--help'):
            usage()
            sys.exit()
        if opt == '--add':
            opt_host = arg
    # listen
    if action == 'listen':
        if opt_host:
            l = ListenerDaemon(more_host=opt_host)
        else:
            l = ListenerDaemon()
        if len(odd_arg_list) > 0:
            # kill the listen daemon
            if odd_arg_list[0] == 'kill':
                l.kill()
                sys.exit()
            # start listen
            if odd_arg_list[0] == 'daemonize':
                l.daemonize()
        else:
            l.main()
    # speak
    elif action == 'speak':
        if odd_arg_list[0] == 'self':
            opt_host = SELF_HOST
        else:
            opt_host = odd_arg_list[0]
        message_list = odd_arg_list[1:]
        talk.speak(message_list, host=opt_host)

#!/usr/bin/env python
""" talk through a socket.socket """

import os
import sys
from getopt import getopt, GetoptError
from signal import SIGTERM
from daemon import DaemonContext
from lockfile.pidlockfile import PIDLockFile

from talk import Listener, speak, SELF_ADDR

PID_FILE = '/var/run/talk.pid'
LOG_FILE = 'talk.log'

class Listend(Listener):
    """ listen through a socket.socket """
    def __init__(self, more_addr=None, pidfile=PID_FILE, logfile=LOG_FILE):
        Listener.__init__(self, more_addr)
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
        for addr in self.addr_list:
            sys.stdout.write('talk listen start @ %s \n' % addr)    
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
    opt_addr = None
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
            opt_addr = arg
    # listen
    if action == 'listen':
        l = Listend(more_addr=opt_addr)
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
            opt_addr = SELF_ADDR
        else:
            opt_addr = odd_arg_list[0]
        message_list = odd_arg_list[1:]
        speak(message_list, addr=opt_addr)

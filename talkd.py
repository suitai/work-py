#!/usr/bin/env python
""" command of talk through a socket """

import os
import sys
import logging
from getopt import getopt,GetoptError
from signal import SIGTERM
try:
    import psutil
    from talk import Listener,speak,LOG_FORMAT
except ImportError, detail:
    sys.exit('ImportError: %s' % detail) 

PID_FILE = '/var/run/talk.pid'
LOG_FILE = './talk.log'

def check_pidfile(pidfile):
    if os.path.exists(pidfile):
        pidfile = open(pidfile, 'r')
        return int(pidfile.read())
    else:
        sys.exit('Listener is not running') 

def kill_process(pidfile):
    """ kill the daemon process"""
    # get pid
    pid = check_pidfile(pidfile)
    # kill
    try:
        os.kill(pid, SIGTERM)
    except OSError, detail:
        sys.exit('OSError: %s' % detail)

def is_process(pidfile):
    # get pid
    pid = check_pidfile(pidfile)
    # search pid
    if pid in psutil.get_pid_list():
        return True
    else:
        return False

def usage():
    command = sys.argv[0]
    sys.stdout.write('usage: %s listen [status|run|daemon|kill]\n' % command)    
    sys.stdout.write('       %s speak <host> <message>\n' % command)    
    sys.exit()

if __name__ == "__main__":
    """ command execution """
    if not len(sys.argv) > 1:
        usage()
    action = sys.argv[1]
    # handle options
    option = "h"
    long_option = ["help",]
    try:
        opt_list, odd_arg_list = getopt(sys.argv[2:], option, long_option)
    except GetoptError, detail:
        sys.exit('GetoptError: %s' % detail)
    for opt, arg in opt_list:
        if opt in ('-h', '--help'):
            usage()
    # listen
    if action == 'listen':
        if len(odd_arg_list) > 0:
            # state of the listen deamon
            if odd_arg_list[0] == 'status':
                status = is_process(PID_FILE)
                if status:
                    sys.stdout.write('Listener is running\n')
                else:
                    sys.exit('Listener is not running')
            # kill the listen daemon
            elif odd_arg_list[0] == 'kill':
                kill_process(PID_FILE)
                sys.stdout.write('Listener killed\n')
            # start daemon
            elif odd_arg_list[0] == 'daemon':
                l = Listener()
                l.daemonize(pidfile=PID_FILE, logfile=LOG_FILE)
                sys.stdout.write('Listener daemon start\n')
            # start listen
            elif odd_arg_list[0] == 'run':
                l = Listener()
                l.start()
            else:
                usage()
        else:
            usage()
    # speak
    elif action == 'speak':
        if len(odd_arg_list) > 0:
            opt_addr = odd_arg_list[0]
            message_list = odd_arg_list[1:]
            speak(message_list, addr=opt_addr)
        else:
            usage()
    else:
        usage()

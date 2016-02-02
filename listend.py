#!/usr/bin/env python
""" command to start a socket listen daemon"""


import os
import sys
import getopt
import socket
import signal
import subprocess

import socketalk

CONFIG_FILE = "listend.yaml"
LOG_FILE = "listend.log"

## functions
def listen(daemonize = False):
    if not os.path.exists(CONFIG_FILE):
        sys.exit("Error: Can not exists %s" % CONFIG_FILE)

    l = socketalk.Listen(CONFIG_FILE)
    try:
        if daemonize:
            l.start_daemon(LOG_FILE)
        else:
            l.start()
    except socket.error, detail:
        sys.exit("Error: %s" % detail)


def kill_daemon():
    if not os.path.exists(socketalk.PID_FILE):
        sys.exit("not running")

    with open(socketalk.PID_FILE, 'r') as file:
        pid = int(file.read())
    try:
        os.kill(pid, signal.SIGTERM)
        os.remove(socketalk.PID_FILE)
    except OSError, detail:
        sys.exit("Error: %s" % detail)


def daemon_status():
    if not os.path.exists(socketalk.PID_FILE):
        sys.exit("not running")

    with open(socketalk.PID_FILE, 'r') as file:
        pid = file.read()
    try:
        open(os.path.join("/proc", pid, "cmdline"), 'rb')
        print "running"
    except IOError:
        sys.exit("not running")


def less_log():
    if not os.path.exists(LOG_FILE):
        sys.exit("Error: Can not exists %s" % LOG_FILE)

    cmd = ['/usr/bin/less', LOG_FILE]
    pid = subprocess.Popen(cmd)

    def send_signal(signum, frame):
        pid.send_signal(signal.SIGINT)

    signal.signal(signal.SIGINT, send_signal)
    pid.communicate()


def usage():
    """ print usage """
    sys.exit("Usage: %s [--daemon|--kill|--status]" % (sys.argv[0]))


def main():
    """ check argv """
    option = 'dh'
    long_option = ["help", "daemon", "kill", "status", "log"]
    # handle options
    try:
        opts, args = getopt.getopt(sys.argv[1:], option, long_option)
    except getopt.GetoptError, detail:
        sys.exit("GetoptError: %s" % detail)
    # check option
    if len(opts) == 0:
        listen()
        return "run"
    elif len(opts) == 1:
        if opts[0][0] == "--daemon":
            listen(daemonize=True)
        if opts[0][0] == "--kill":
            kill_daemon()
        if opts[0][0] == "--status":
            daemon_status()
        elif opts[0][0] == "--log":
            less_log()
        else:
            usage()
    else:
        usage()


if __name__ == '__main__':
    main()

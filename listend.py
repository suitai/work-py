#!/usr/bin/env python
""" command to start a socket listen daemon"""

import sys
import getopt

import socketalk


def usage():
    """ print usage """
    sys.exit("Usage: %s [-d|--kill|--status]" % (sys.argv[0]))

def check_opts():
    """ check argv """
    option = 'dh'
    long_option = ["help", "kill", "status", "log"]
    # handle options
    try:
        opts, args = getopt.getopt(sys.argv[1:], option, long_option)
    except getopt.GetoptError, detail:
        sys.exit("GetoptError: %s" % detail)
    # check option
    if len(opts) == 0:
        return "run"
    elif len(opts) == 1:
        if opts[0][0] == "--kill":
            return "kill"
        if opts[0][0] == "--status":
            return "status"
        elif opts[0][0] == "--log":
            return "log"
        elif opts[0][0] == "-d":
            return "rund"
        else:
            return "usage"
    else:
        return "usage"

functions = {'run': socketalk.listen,
             'rund': socketalk.listend,
             'kill': socketalk.kill_listend,
             'status': socketalk.status_listend,
             'log': socketalk.less_log,
             'usage': usage}

def main():
    action = check_opts()
    functions[action]()

if __name__ == '__main__':
    main()

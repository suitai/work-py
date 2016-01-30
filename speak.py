#!/usr/bin/env python
""" command to speak to a socket """

import sys
import getopt

import socketalk

def usage():
    """ print usage """
    sys.exit("Usage: %s \"message\"\n" % sys.argv[0])

def check_opts():
    """ check argv """
    option = 'h'
    long_option = ["help"]
    # handle options
    try:
        opts, args = getopt.getopt(sys.argv[1:], option, long_option)
    except getopt.GetoptError, detail:
        raise Exception('GetoptError: %s' % detail)
    # check option
    for opt, arg in opts:
        if opt in ["-h", "--help"]:
            usage()

    return " ".join(args)

def main():
    message = check_opts()
    socketalk.speak(message)

if __name__ == '__main__':
    main()

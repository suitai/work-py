#!/usr/bin/env python
""" command to speak to a socket"""

import sys
from getopt import getopt, GetoptError
try:
    from socketalk import SocketTalk
except ImportError, detail:
    sys.exit('ImportError: %s' % detail)


class SpeakCommand:
    def __init__(self, port=SocketTalk.default_port):
        self.port = port
        self.talk = SocketTalk(logfile='/dev/null')

    def usage(self):
        sys.exit('Usage: speak [-p port] hostname message\n')

    def send(self, host, message):
        socket = '%s:%d' % (host, self.port)
        return self.talk.speak(socket, message)

if __name__ == '__main__':
    """ command execution """
    s = SpeakCommand()
    # handle options
    option = "p:"
    long_option = ["help", ]
    try:
        (opt_list, args) = getopt(sys.argv[1:], option, long_option)
    except (GetoptError, detail):
        sys.exit('GetoptError: %s' % detail)
    # check option
    for (opt, arg) in opt_list:
        if opt == '-p':
            s.port = int(arg)
        elif opt == '--help':
            s.usage()
    # speak
    if len(args) > 1:
        host = args[0]
        messages = args[1:]
        message = ' '.join(messages)
        reply = s.send(host, message)
        sys.stdout.write('%s\n' % reply)
    else:
        s.usage()

#!/usr/bin/env python
""" command to speak to a socket"""

import sys
from getopt import getopt, GetoptError
try:
    from socketalk import SocketTalk, TalkError, DEFAULT_PORT
except ImportError, detail:
    sys.exit('ImportError: %s' % detail)


class SpeakCommand(object):
    """ check argv and send message """

    option = 'p:'
    long_option = ['help', ]

    def __init__(self, argv):
        self.name = argv[0]
        self.argv = argv[1:]
        self.port = DEFAULT_PORT
        self.host = None
        self.messages = []

    def check_argv(self):
        """ check argv """
        # handle options
        try:
            (opt_list, other_args) = getopt(self.argv, self.option,
                                            self.long_option)
        except GetoptError, detail:
            sys.exit('GetoptError: %s' % detail)
        # check option
        for (opt, arg) in opt_list:
            if opt == '-p':
                self.port = int(arg)
            else:
                return 'usage'
        # check other args
        if len(other_args) > 1:
            self.host = other_args[0]
            self.messages = other_args[1:]
            return 'send'
        else:
            return 'usage'

    def usage(self):
        """ print usage """
        sys.exit('Usage: %s [-p port] hostname message\n' % self.name)

    def send(self):
        """ send message """
        reply = ''
        socket = '%s:%d' % (self.host, self.port)
        message = ' '.join(self.messages)
        talk = SocketTalk(logfile='/dev/null')
        try:
            reply = talk.speak(socket, message)
        except TalkError, detail:
            sys.exit('Error: %s' % detail)
        sys.stdout.write('%s\n' % reply)

    def main(self):
        operate = self.check_argv()
        if operate == 'send':
            self.send()
        else:
            self.usage()


if __name__ == '__main__':
    SPEAK = SpeakCommand(sys.argv)
    SPEAK.main()

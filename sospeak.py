#!/usr/bin/env python
""" command to speak to a socket """

import sys
import socket
import getopt

DEFAULT_PORT = 50010
BUFFER_SIZE = 1024


def speak(message, host, port):
    """ speak through a socket """
    # send message
    speak_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    speak_socket.settimeout(5)
    try:
        speak_socket.connect((host, port))
        speak_socket.send(message)
    except socket.error:
        speak_socket.close()
    except socket.timeout:
        speak_socket.close()
    else:
        # accept message
        message = speak_socket.recv(BUFFER_SIZE)  # bufsize
        return message


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
            (options, others) = getopt.getopt(self.argv, self.option,
                                              self.long_option)
        except getopt.GetoptError, detail:
            sys.exit('GetoptError: %s' % detail)
        # check option
        for (opt, arg) in options:
            if opt == '-p':
                self.port = int(arg)
            else:
                return 'usage'
        # check other args
        if len(others) > 1:
            self.host = others[0]
            self.messages = others[1:]
            return 'speak'
        else:
            return 'usage'

    def usage(self):
        """ print usage """
        sys.exit('Usage: %s [-p port] hostname message\n' % self.name)

    def speak(self):
        """ send message """
        reply = ''
        message = ' '.join(self.messages)
        try:
            reply = speak(message, self.host, self.port)
        except Exception, detail:
            sys.exit('Error: %s' % detail)
        sys.stdout.write('%s\n' % reply)

    def main(self):
        operate = self.check_argv()
        if operate == 'speak':
            self.speak()
        else:
            self.usage()


if __name__ == '__main__':
    SPEAK = SpeakCommand(sys.argv)
    SPEAK.main()

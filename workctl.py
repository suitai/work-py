#!/usr/bin/env python
""" command of talk through a socket """

import sys
from getopt import getopt, GetoptError
try:
    from talk import Conversation
    from workd import ListenDaemon
except ImportError, detail:
    sys.exit('ImportError: %s' % detail)


class ManageRequest:

    def __init__(self):
        self.conv = Conversation()
        self.queue = ListenDaemon.queue
        self.socket = ListenDaemon.socket
        self.operate = None
        self.target = None
        self.reply = None

    def usage(self):
        sys.stdout.write('Usage: workctl [socket|queue|worker|job]\n')

    def request(self, messages):
        message_len = len(messages)
        if message_len > 1:
            self.operate = messages[1]
        if message_len > 2:
            self.target = messages[2]
        message = ' '.join(messages)
        reply = self.conv.speak(message, self.socket)
        return reply

    def refer_socket(self, reply):
        # socket info
        result = ('Usage: socket [list]\n'
                  '              [add <addres>]\n'
                  '              [del <addres>]\n')
        if self.operate:
            if self.operate == 'list' and reply:
                results = ['Addres           Status',
                           '---------------------------------']
                for line in reply.split('\n'):
                    words = line.split()
                    addr = '{0:<16}'.format(words[0])
                    status = '{0:<16}'.format(words[1])
                    results.append('%s %s' % (addr, status))
                result = '\n'.join(results) + '\n'
            elif self.operate in ['add', 'del']:
                result = ('Usage: socket %s <addres>' % self.operate)
                if self.target:
                    if reply:
                        result = ('%s the "%s" socket\n'
                                  % (self.operate, self.target))
                    else:
                        result = ('Error: cannot %s the "%s" socket\n'
                                  % (self.operate, self.target))
        return result

    def refer_queue(self, reply):
        # manage queue
        result = ('Usage: queue [list]\n'
                  '             [add <name>]\n'
                  '             [del <name>]\n')
        if self.operate:
            if self.operate == 'list' and reply:
                results = ['Name             Status',
                           '---------------------------------']
                for line in reply.split('\n'):
                    words = line.split()
                    name = '{0:<16}'.format(words[0])
                    status = '{0:<16}'.format(words[1])
                    results.append('%s %s' % (name, status))
                result = '\n'.join(results) + '\n'
            elif self.operate in ['add', 'del']:
                result = ('Usage: queue %s <addres>\n' % self.operate)
                if self.target:
                    if reply:
                        result = ('%s the "%s" queue\n'
                                  % (self.operate, self.target))
                    else:
                        result = ('Error: cannot %s the "%s" queue\n'
                                  % (self.operate, self.target))
        return result

    def refer_worker(self, reply):
        # manage worker
        result = ('Usage: worker [list]\n'
                  '              [add <name>]\n'
                  '              [del <name>]\n')
        if self.operate:
            if self.operate == 'list' and reply:
                results = ['Name             Status',
                           '---------------------------------']
                for line in reply.split('\n'):
                    words = line.split()
                    name = '{0:<16}'.format(words[0])
                    status = '{0:<16}'.format(words[1])
                    results.append('%s %s' % (name, status))
                result = '\n'.join(results) + '\n'
            elif self.operate in ['add', 'del']:
                result = ('Usage: worker %s <addres>\n' % self.operate)
                if self.target:
                    if reply:
                        result = ('%s the "%s" worker\n'
                                  % (self.operate, self.target))
                    else:
                        result = ('Error: cannot %s the "%s" worker\n'
                                  % (self.operate, self.target))
        return result

if __name__ == '__main__':
    """ command execution """
    result = 'Usage: workctl [socket|queue|worker|job]\n'
    # handle options
    option = "h"
    long_option = ["help", ]
    try:
        opt_list, args = getopt(sys.argv[1:], option, long_option)
    except (GetoptError, detail):
        sys.exit('GetoptError: %s' % detail)
    for opt, arg in opt_list:
        if opt in ('-h', '--help'):
            sys.exit(result)
    # speak
    if len(args) > 0:
        messages = args[0:]
        manage = ManageRequest()
        reply = manage.request(messages)
        # check
        operate = {'socket': manage.refer_socket,
                   'queue': manage.refer_queue,
                   'worker': manage.refer_worker}
        if messages[0] in operate.keys():
            result = operate.get(messages[0])(reply)
    sys.stdout.write('%s' % result)

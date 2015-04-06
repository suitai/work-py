#!/usr/bin/env python

import sys

try:
    from socketalk import TalkError
    from solistend import ListenCommand
    from storework import StoreClerk
except ImportError, detail:
    sys.exit('ImportError: %s' % detail)


class ClerkCommand(ListenCommand):

    pidfile = '/var/run/clerkd.pid'
    logfile = '/var/log/clerkd.log'

    def listen(self):
        clerk = StoreClerk(db_url=self.database)
        try:
            clerk.listen()
        except TalkError, detail:
            sys.exit('Error: %s' % detail)


def main():
    clerk = ClerkCommand(sys.argv)
    operate = clerk.check_argv()
    if operate == 'run':
        clerk.run()
    elif operate == 'kill':
        clerk.kill()
    elif operate == 'log':
        clerk.log()
    else:
        clerk.usage()

if __name__ == '__main__':
    main()

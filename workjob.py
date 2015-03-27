#!/usr/bin/env python
""" command of talk through a socket """
import os
import sys
from getopt import getopt, GetoptError
from talk import Conversation
from workd import WorkJob, ListenDaemon


class SubjobManager():

    def __init__(self):
        self.conv = Conversation()
        self.queue = ListenDaemon.queue
        self.socket = ListenDaemon.socket

    def usage(self):
        sys.exit('Usage: wjob submit <command>\n'
                 '            delete <jobid>\n')

    def submit(self, messages):
        if len(messages) > 0:
            # get info
            message = ('job add %s' % WorkJob().get_job_message(self.queue,
                                                                messages))
            reply = self.conv.speak(message, self.socket)
            sys.stdout.write('JobID %s\n' % reply)
        else:
            sys.exit('Usage: wjob submit <command>')

    def delete(self, messages):
        if len(messages) > 0:
            uid = os.getuid()
            jobid = int(messages[0])
            message = ('job info %d' % jobid)
            reply = self.cpnv.speak(message, self.socket)
            if int(reply.split()[0]) == uid:
                message = ('job del %d' % jobid)
                reply = self.conv.speak(message, self.socket)
            if reply:
                sys.stdout.write('delete the %d job\n' % jobid)
            else:
                sys.exit('Error: You cannot delete the %d job', jobid)
        else:
            sys.exit('Usage: wjob delete <jobid>')

    def info(self, messages):
        if len(messages) > 0:
            uid = os.getuid()
            jobid = int(messages[0])
            message = ('job info %d' % jobid)
            reply = self.conv.speak(message, self.socket)
        if reply:
            if int(reply.split()[0]) == uid:
                words = reply.split()
                sys.stdout.write('id: %s\n'
                                 'user: %s %s\n'
                                 'host: %s\n'
                                 'queue: %s\n'
                                 'command: %s\n'
                                 'status: %s\n'
                                 % (jobid, words[0], words[1], words[2],
                                    words[3], words[4], words[5]))
            else:
                sys.exit('Error: You cannot know about the %d job' % jobid)
        else:
            sys.exit('Usage: wjob info <jobid>')

    def list(self):
        message = ('job list')
        reply = self.conv.speak(message, self.socket)

        if reply:
            results = ['ID  User     Queue            Status',
                       '----------------------------------------------']
            for line in reply.split('\n'):
                words = line.split()
                ident = '{0:>3}'.format(words[0])
                user = '{0:<8}'.format(words[1])
                if len(user) > 8:
                    user = [user[i] for i in range(0, 8)]
                queue = '{0:<16}'.format(words[2])
                if len(queue) > 16:
                    queue = [queue[i] for i in range(0, 16)]
                status = '{0:<16}'.format(words[3])
                results.append('%s %s %s %s' % (ident, user, queue, status))
            result = '\n'.join(results)
            sys.stdout.write('%s\n' % result)


if __name__ == '__main__':
    """ command execution """
    job = SubjobManager()
    # handle options
    option = 'h'
    long_option = ['help', ]
    try:
        opt_list, args = getopt(sys.argv[1:], option, long_option)
    except GetoptError, detail:
        sys.exit('GetoptError: %s' % detail)
    for opt, arg in opt_list:
        if opt in ('-h', '--help'):
            job.usage()

    if len(args) > 0:
        operate = {'submit': job.submit,
                   'delete': job.delete}
        show = {'info': job.info,
                'list': job.list}
        messages = args[1:]
        if messages:
            if args[0] in operate.keys():
                operate.get(args[0])(messages)
        elif args[0] in show.keys():
            show.get(args[0])()
        else:
            job.usage()
    else:
        job.usage()

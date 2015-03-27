#!/usr/bin/env python

import os
import sys
import pwd
import time
from subprocess import Popen
from signal import SIGTERM
from socket import gethostbyname, gethostname

try:
    import psutil
    from talk import Conversation
except ImportError, detail:
    sys.exit('ImportError: %s' % detail)


class WorkManager(Conversation):
    """ manage message for jobs """
    jobdir = '/var/tmp/jobs'
    workdir = '/var/tmp/work'

    def __init__(self):
        super(WorkManager, self).__init__()
        self.jobs = {}
        if not os.path.exists(self.jobdir):
            os.mkdir(self.jobdir)
        if not os.path.exists(self.workdir):
            os.mkdir(self.workdir)

    def manage_fn(self, message):
        """ check messages to manage talkmanager"""
        reply = ''
        messages = message.split()
        message_len = len(messages)
        if message_len > 0:
            target = {'socket': self._manage_socket,
                      'queue': self._manage_queue,
                      'worker': self._manage_worker,
                      'job': self._manage_job}
            if messages[0] in target.keys():
                self.logger.info('manage %s %s' % (messages[0], messages[1]))
                reply = target.get(messages[0])(messages)
        return reply

    def _manage_socket(self, messages):
        reply = ''
        message_len = len(messages)
        # socket info
        if message_len == 1:
            if messages[1] == 'list':
                list = []
                for (addr, listener) in self.listeners.iteritems():
                    list.append('%s %s' % (addr, listener.status))
                reply = ('%s' % '\n'.join(list))
                self.logger.debug('send socket list')
        elif message_len > 1:
            if messages[1] == 'add':
                if self.add_listener(messages[2]):
                    reply = messages[2]
            elif messages[1] == 'del':
                if self.del_listener(messages[2]):
                    reply = messages[2]
        return reply

    def _manage_queue(self, messages):
        reply = ''
        message_len = len(messages)
        # manage queue
        if message_len == 1:
            if messages[1] == 'list':
                list = []
                for name in sorted(self.queues.keys()):
                    list.append('%s %s' % (name, self.queues[name].status))
                reply = ('%s' % '\n'.join(list))
                self.logger.debug('send queue list')
        elif message_len > 1:
            if messages[1] == 'add':
                if self.add_queue(messages[2]):
                    reply = messages[2]
            elif messages[1] == 'del':
                if self.del_queue(messages[2]):
                    reply = messages[2]
        return reply

    def _manage_worker(self, messages):
        reply = ''
        message_len = len(messages)
        # manage worker
        if message_len == 1:
            if messages[1] == 'list':
                list = []
                for (name, worker) in self.workers.iteritems():
                    list.append('%s %s' % (name, worker.status))
                reply = ('%s' % '\n'.join(list))
                self.logger.debug('send worker list')
        elif message_len > 1:
            if messages[1] == 'add':
                if self.add_worker(messages[2]):
                    reply = messages[2]
            elif messages[1] == 'del':
                if self.del_worker(messages[2]):
                    reply = messages[2]
        return reply

    def _manage_job(self, messages):
        reply = ''
        message_len = len(messages)
        # manage worker
        if message_len == 1:
            if messages[1] == 'list':
                list = []
                for ident in sorted(self.jobs):
                    info = self.jobs[ident].get_info('short')
                    list.append('%s' % (ident, info))
                reply = ('%s' % '\n'.join(list))
                self.logger.debug('send job list')
        elif message_len > 1:
            if messages[1] == 'info' and messages[2] in self.jobs:
                reply = self.jobs[messages[2]].get_info()
                self.logger.debug('send job %s info' % ident)
            elif messages[1] == 'add':
                jobid = self.add_job(messages[2:])
                if jobid > 0:
                    reply = ('%d' % jobid)
            elif messages[1] == 'del':
                if self.del_job(messages[2]):
                    reply = messages[2]
        return reply

    def add_job(self, messages):
        # check messages
        job_info = WorkJob()
        job_info.set_info_from_messages(messages)
        # add job
        if len(job_info.queue) > 0 and len(job_info.job) > 0:
            job_id = self.enqueue_message(job_info.queue, job_info.job)
            if job_id > 0:
                with self.list_condition:
                    self.jobs[str(job_id)] = job_info
                self.logger.info('add job %d' % job_id)
                return job_id
        return -1

    def del_job(self, job_id):
        for name in self.jobs:
            if name == job_id:
                if self.jobs[str(job_id)].returncode:
                    try:
                        os.kill(self.jobs[str(job_id)].pid, SIGTERM)
                    except OSError:
                        return False
                for queue_name in self.queues:
                    if self.queues[queue_name].del_item(int(job_id)):
                        break
                with self.list_condition:
                    del self.jobs[str(job_id)]
                self.logger.info('del job %d' % job_id)
                return True
        return False

    def work_fn(self, queue_name, message, ident):
        """ start work """
        ident = str(ident)
        ident_name = ident.zfill(3)
        log_prefix = ('[%s-%s]' % (queue_name, ident_name))
        # dir
        job = self.jobs[ident]
        workdir = ('%s/%s' % (self.workdir, job.user))
        if not os.path.exists(workdir):
            os.mkdir(workdir)
        # write job script
        job_script = ('%s/job-%s.sh' % (self.jobdir, ident_name))
        with open(job_script, 'w') as f:
            f.write(message)
        os.chmod(job_script, 0755)
        # open job out file
        job_out = ('%s/job-%s.out' % (self.jobdir, ident_name))
        with open(job_out, 'w') as f:
            try:
                # popen
                self.logger.info('%s job start' % log_prefix)
                p = Popen(job_script, shell=True, stdout=f, stderr=f,
                          cwd=workdir, preexec_fn=self.preexec(job))
            except OSError, detail:
                self.logger.error('OSError: %s', detail)
            else:
                self.logger.debug('%s popen "%s"' % (log_prefix, message))
                self.jobs[ident].status = 'Run'
                self.jobs[ident].pid = p.pid
                p.communicate()
                # return
                self.jobs[ident].returncode = p.returncode
                self.logger.info('%s job end %d' % (log_prefix, p.returncode))
        if p.returncode == 0:
            self.jobs[ident].status = 'Finish'
        else:
            self.jobs[ident].status = 'Error'

    def preexec(self, job):
        uid = job.uid

        def set_uid():
            os.setuid(uid)
        return set_uid


class WorkJob:
    def __init__(self):
        self.uid = 0
        self.job = ''
        self.user = ''
        self.host = ''
        self.queue = ''
        self.status = 'Queued'
        self.pid = None
        self.returncode = None

    def get_info(self, length=long):
        uid = self.uid
        user = self.user
        host = self.host
        queue = self.queue
        command = self.job.split()[0]
        status = self.status
        if length == 'long':
            info = ('%d %s %s %s %s %s'
                    % (uid, user, host, queue, command, status))
        elif length == 'short':
                info = ('%s %s %s'
                        % (user, queue, status))
        return info

    def set_info_from_messages(self, messages):
        for num, word in enumerate(messages):
            if word == 'UID':
                self.uid = int(messages[num + 1])
            elif word == 'USER':
                self.user = (messages[num + 1])
            elif word == 'HOST':
                self.host = (messages[num + 1])
            elif word == 'QUEUE':
                self.queue = messages[num + 1]
            elif word == 'JOB':
                self.job = ' '.join(messages[num + 1:])

    def get_job_message(self, queue, messages):
        queue = queue
        uid = os.getuid()
        user = pwd.getpwuid(uid)[0]
        host = gethostbyname(gethostname())
        job = ' '.join(messages[0:])
        # speak
        message = ('QUEUE %s UID %s USER %s HOST %s JOB %s'
                   % (queue, uid, user, host, job))
        return message


class ListenDaemon():

    pidfile = '/var/run/workd.pid'
    logfile = '/var/log/workd.log'
    idfile = '/var/run/work.id'
    queue = '05_default'
    socket = '127.0.0.1:50010'
    worker = 'Johnathan'

    def __init__(self):
        pass

    def usage(self):
        sys.stdout.write('Usage: workd [start|status|stop]\n')

    def start(self):
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        worker = WorkManager()
        worker.prepare_listen(self.idfile)
        worker.add_queue(self.queue)
        worker.add_listener(self.socket, worker.manage_fn)
        worker.add_worker(self.worker)
        worker.daemonize_listen(pidfile=self.pidfile, logfile=self.logfile)

    def stop(self):
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        pid = _get_pid_from_file(self.pidfile)
        try:
            os.kill(pid, SIGTERM)
        except OSError, detail:
            sys.exit('Error: Cannot kill Worker: %s' % detail)
        while os.path.exists(self.pidfile):
            time.sleep(1)

    def restart(self):
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        self.stop()
        self.start()

    def status(self):
        pid = _get_pid_from_file(self.pidfile)
        if pid in psutil.get_pid_list():
            sys.stdout.write('Info: Worker is running\n')
        else:
            sys.exit('Error: Worker is not running')

    def run(self):
        if not os.getuid() == 0:
            sys.exit('Error: You must be root')
        worker = WorkManager()
        worker.prepare_listen(self.idfile)
        worker.add_queue(self.queue)
        worker.add_listener(self.socket, worker.manage_fn)
        worker.add_worker(self.worker)
        worker.start_listen()


def _get_pid_from_file(pidfile):
    if os.path.exists(pidfile):
        with open(pidfile, 'r') as f:
            return int(f.read())
    else:
        sys.exit('Info: Worker is not running')


if __name__ == "__main__":
    d = ListenDaemon()
    if len(sys.argv) > 1:
        operate = {'start': d.start,
                   'stop': d.stop,
                   'restart': d.restart,
                   'status': d.status,
                   'run': d.run}
        if sys.argv[1] in operate.keys():
            operate.get(sys.argv[1], d.usage)()
    else:
        d.usage()

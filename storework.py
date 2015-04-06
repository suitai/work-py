
from multiprocessing import cpu_count
from socketalk import SocketTalk, add_thread
from datautil import TalkDB, MessageData


class StoreClerk(SocketTalk):

    def __init__(self, logfile='/dev/stdout', loglevel='debug',
                 db_url='sqlite://'):
        super(StoreClerk, self).__init__(logfile, loglevel)
        self.talk_db = TalkDB(db_url)
        self.max_works = cpu_count()
        self.num_works = 0

    def main_loop(self):
        # delete finished message
        messages = self.talk_db.get(MessageData)
        self.logger.info('%s' % messages)
        # start work
        self.start(messages)
        with self.condition:
            self.condition.wait(3)

    def start(self, messages):
        for message in messages:
            if (message.status == 'Init' and self.num_works < self.max_works):
                message.status == 'Running'
                self.talk_db.commit()
                add_thread(self.work, args=(message, ))

    def work(self, message):
        self.logger.info('start work [%d]', message.id)
        message.status = 'Finished'
        self.talk_db.commit()
        with self.condition:
            self.condition.notify()

if __name__ == '__main__':
    pass

import threading
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.schema import Column, ForeignKey
from sqlalchemy.types import Integer, String
from sqlalchemy.orm import relation
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SimpleData(Base):
    __tablename__ = 'single'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    status = Column(String)
    #owner_id = Column(Integer, ForeignKey('note.id'))

    def __init__(self, name):
        self.name = name
        self.status = 'Init'

    def __repr__(self):
        return ('SingleData(%s)' % (self.name))


class SocketData(Base):
    __tablename__ = 'socket'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    status = Column(String)

    def __init__(self, name):
        self.name = name
        self.status = 'Init'

    def __repr__(self):
        return ('SocketData(%s)' % (self.name))


class WorkerData(Base):
    __tablename__ = 'worker'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    status = Column(String)

    def __init__(self, name):
        self.name = name
        self.status = 'Init'

    def __repr__(self):
        return ('WorkerData(%s)' % (self.name))

"""
class LabelData(SimpleData):
    __tablename__ = 'label'

    def __repr__(self):
        return ('LabelData(%s)' % (self.name))
"""


class NoteData(Base):
    __tablename__ = 'note'
    id = Column(Integer, primary_key=True)
    message = Column(String)
    """
    label = relation('DataLabel', order_by='DataLabel.id',
                     uselist=True, backref='note')
    worker = relation('DataWorker', order_by='DataWorker.id',
                      uselist=True, backref='note')
    origin = relation('DataSocket', order_by='DataSocket.id',
                      uselist=True, backref='note')
    master = relation('DataSocket', order_by='DataSocket.id',
                      uselist=True, backref='note')
    """
    def __init__(self, message, label, worker=None, origin=None, master=None):
        self.message = message
        self.label = label
        self.worker = worker
        self.origin = origin
        self.master = master

    def __repr__(self):
        return ('<NoteData(%d, %s, %s, %s, %s)>'
                % (self.id, self.message, self.label, self.worker,
                   self.origin, self.master))


class TalkDB:
    def __init__(self, url='sqlite://'):
        self.engine = create_engine(url, echo=True)
        Base.metadata.create_all(self.engine)
        SessionMaker = sessionmaker(bind=self.engine)
        self.session = SessionMaker()

    def add_data(self, data, name):
        target = self.session.query(data).filter_by(name=name).first()
        if not target:
            new = data(name)
            self.session.add(new)
            return new
        return None

    def del_data(self, data, name):
        target = self.session.query(data).filter_by(name=name).first()
        if target:
            self.session.delete(target)
            return target
        return None

    def get_data(self, data, name):
        target = self.session.query(data).filter_by(name).first()
        return target

    def get_list(self, data):
        list = self.session.query(data)
        return list

    def commit(self):
        self.session.commit()

if __name__ == '__main__':
    pass

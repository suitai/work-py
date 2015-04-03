from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.schema import Column, ForeignKey
from sqlalchemy.types import Integer, String
from sqlalchemy.orm import relation
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SocketData(Base):
    __tablename__ = 'socket'
    id = Column(Integer, primary_key=True)
    addr = Column(String)

    def __init__(self, addr):
        self.addr = addr

    def __repr__(self):
        return ('SocketData(%s)' % (self.addr))


class MessageData(Base):
    __tablename__ = 'message'
    id = Column(Integer, primary_key=True)
    text = Column(String)
    origin = Column(String)

    def __init__(self, text, origin=None):
        self.text = text
        self.origin = origin

    def __repr__(self):
        return ('WorkerData(%s, %s)' % (self.text, self.origin))


class TalkDB:
    def __init__(self, url='sqlite://', echo=False):
        self.engine = create_engine(url, echo=echo)
        Base.metadata.create_all(self.engine)
        SessionMaker = sessionmaker(bind=self.engine)
        self.session = SessionMaker()

    def add(self, data):
        self.session.add(data)

    def delete(self, data):
        self.session.delete(data)

    def get(self, data, key=None):
        if key:
            target = self.session.query(data).filter_by(key).first()
        else:
            target = self.session.query(data)
        return target

    def commit(self):
        self.session.commit()

if __name__ == '__main__':
    pass

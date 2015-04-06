from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.schema import Column, ForeignKey
from sqlalchemy.types import Integer, String
from sqlalchemy.orm import relation
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class MessageData(Base):
    __tablename__ = 'message'
    id = Column(Integer, primary_key=True)
    text = Column(String)
    origin = Column(String)
    status = Column(String)

    def __init__(self, text, origin=None):
        self.text = text
        self.origin = origin
        self.status = 'Init'

    def __repr__(self):
        return('WorkerData(%d, %s, %s, %s)'
               % (self.id, self.text, self.origin, self.status))


class TalkDB:
    def __init__(self, url='sqlite://', echo=False):
        self.engine = create_engine(url, echo=echo)
        Base.metadata.create_all(self.engine)
        SessionMaker = sessionmaker(bind=self.engine)
        self.session = SessionMaker()

    def add(self, object):
        self.session.add(object)

    def delete(self, object):
        self.session.delete(object)

    def get(self, data):
        objects = self.session.query(data).all()
        return objects

    def commit(self):
        self.session.commit()

if __name__ == '__main__':
    pass

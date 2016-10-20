import datetime
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    ForeignKey,
    Text,
    DateTime,
    SmallInteger,
    PrimaryKeyConstraint
)
from sqlalchemy.orm import relationship


class BaseModel(object):

    __bind_key__ = 'DEFAULT'

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()


Base = declarative_base(cls=BaseModel)


class Post(Base):
    id = Column(Integer, primary_key=True)
    title = Column(String(256))
    text = Column(Text)
    post_date = Column(DateTime)
    last_modified_date = Column(DateTime)

    # if 1 then make it a draft
    draft = Column(SmallInteger, default=0)

    tag_posts = relationship('Tag_Posts',
                             back_populates='post',
                             cascade="all, delete-orphan")

    user_posts = relationship('User_Posts',
                              uselist=False,
                              back_populates='post',
                              cascade="all, delete-orphan")

    def __init__(self, title, text,
                 draft=False,
                 post_date=None,
                 last_modified_date=None):
        self.update(title, text, draft, post_date, last_modified_date)

    def update(self, title, text,
               draft=False,
               post_date=None,
               last_modified_date=None):
        current_datetime = datetime.datetime.utcnow()
        if post_date is not None:
            self.post_date = post_date
        else:
            self.post_date = current_datetime

        if last_modified_date is not None:
            self.last_modified_date = last_modified_date
        else:
            self.last_modified_date = current_datetime
        self.title = title
        self.text = text
        self.draft = 1 if draft is True else 0


class Tag(Base):
    id = Column(Integer, primary_key=True)
    text = Column(String(128), unique=True, index=True)

    def __init__(self, text):
        self.text = text.upper()


class Tag_Posts(Base):
    tag_id = Column(Integer, ForeignKey('tag.id', ondelete='CASCADE'), primary_key=True )
    tag = relationship(Tag, backref='tag')

    post_id = Column(Integer, ForeignKey('post.id', ondelete='CASCADE'), primary_key=True)
    post = relationship(Post, backref='post')


    def __init__(self, post_id, tag_id):
        self.post_id = post_id
        self.tag_id = tag_id

class User_Posts(Base):
    user_id = Column(String(128), index=True, primary_key=True)
    post_id = Column(Integer, ForeignKey('post.id', ondelete='CASCADE'), primary_key=True )
    post = relationship(Post)

    def update(self, user_id):
        self.user_id = user_id

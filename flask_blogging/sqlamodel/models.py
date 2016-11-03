import datetime
from sqlalchemy.ext.declarative import (
    declarative_base,
    declared_attr,
    as_declarative
)
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


def table_dict(prefix=None):
    if prefix is None:
        prefix=''
    tables = ['Post','Tag', 'Tag_Posts', 'User_Posts']
    return {t:prefix.lower()+t.lower() for t in tables}


def Post_Table(Base, prefix=None, bind_key=None):
    d=table_dict(prefix)

    class Post(Base):
        __tablename__ = d['Post']
        __bind_key__ = bind_key
        id = Column(Integer, primary_key=True)
        title = Column(String(256))
        text = Column(Text)
        post_date = Column(DateTime)
        last_modified_date = Column(DateTime)

        # if 1 then make it a draft
        draft = Column(SmallInteger, default=0)

        tag_posts = relationship('Tag_Posts',
                                 back_populates=d['Post'],
                                 cascade="all, delete-orphan")

        user_posts = relationship('User_Posts',
                                  uselist=False,
                                  back_populates=d['Post'],
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

    return Post


def Tag_Table(Base, prefix=None, bind_key=None):
    d=table_dict(prefix)

    class Tag(Base):
        __tablename__ = d['Tag']
        __bind_key__ = bind_key
        id = Column(Integer, primary_key=True)
        text = Column(String(128), unique=True, index=True)

        def __init__(self, text):
            self.text = text.upper()

    return Tag


def Tag_Posts_Table(Base, prefix=None, bind_key=None):
    d=table_dict(prefix)

    class Tag_Posts(Base):
        __tablename__ = d['Tag_Posts']
        __bind_key__ = bind_key

        tag_id = Column(Integer,
                        ForeignKey(d['Tag']+'.id', ondelete='CASCADE'),
                        primary_key=True )
        tag = relationship('Tag', backref=d['Tag'])

        post_id = Column(Integer,
                         ForeignKey(d['Post']+'.id', ondelete='CASCADE'),
                         primary_key=True)
        post = relationship('Post', backref=d['Post'])

        def __init__(self, post_id, tag_id):
            self.post_id = post_id
            self.tag_id = tag_id

    return Tag_Posts


def User_Posts_Table(Base, prefix=None, bind_key=None):
    d=table_dict(prefix)

    class User_Posts(Base):
        __tablename__ = d['User_Posts']
        __bind_key__ = bind_key

        user_id = Column(String(128), index=True, primary_key=True)
        post_id = Column(Integer,
                         ForeignKey(d['Post']+'.id', ondelete='CASCADE'),
                         primary_key=True )
        post = relationship('Post')

        def update(self, user_id):
            self.user_id = user_id

    return User_Posts

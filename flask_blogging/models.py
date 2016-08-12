import logging
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    SmallInteger,
    ForeignKey,
    UniqueConstraint,
    PrimaryKeyConstraint
)
from sqlalchemy.ext.declarative import declarative_base, declared_attr


Base = declarative_base()


def build_table(cls, prefix, bind_key):
    info = {}
    if bind_key is not None:
        info.update({'__bind_key__': bind_key})
    if prefix is not None:
        info.update({'__prefix__': prefix})
    table = type(cls.__name__, (cls,), info)
    return table


class DynamicName(object):

    __prefix__ = None

    def _tablename(tablename, prefix):
        if prefix is None:
            return tablename.lower()
        else:
            return prefix.lower() + tablename.lower()

    @declared_attr
    def __tablename__(cls):
        return cls._tablename(cls.__name__, cls.__prefix__)


class Post(DynamicName, Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    title = Column(String(256))
    text = Column(Text)
    post_date = Column(DateTime)
    last_modified_date = Column(DateTime)

    # if 1 then make it a draft
    draft = Column(SmallInteger, default=0)


class Tag(DynamicName, Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    text = Column(String(128), unique=True, index=True)


class Tag_Posts(DynamicName, Base):
    __abstract__ = True
    __table_args__ = (PrimaryKeyConstraint('tag_id', 'post_id', name='uix_1'),)

    @declared_attr
    def tag_id(cls):
        return Column(Integer,
                      ForeignKey('tag' + '.id',
                                 onupdate="CASCADE",
                                 ondelete="CASCADE"), index=True)

    @declared_attr
    def post_id(cls):
        return Column(Integer,
                      ForeignKey('post' + '.id',
                                 onupdate="CASCADE",
                                 ondelete="CASCADE"), index=True)


class User_Posts(DynamicName, Base):
    __abstract__ = True
    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'post_id', name='uix_2'),
    )

    user_id = Column(String(128), index=True)

    @declared_attr
    def post_id(cls):
        return Column(Integer,
                      ForeignKey('post' + '.id',
                                 onupdate="CASCADE",
                                 ondelete="CASCADE"), index=True)

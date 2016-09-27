import datetime
from . import db
from sqlalchemy.ext.declarative import declared_attr


def build_model(cls, info={}):
    """
    this is a dynamic type constructor
    passing in the class type and a hash of name value pairs
    class is constructed and properties set
    """
    return type(cls.__name__, (cls,), info)


class DynamicName(object):
    """
    a base class to implement naming of the model's tablename
    used to override the default tablename with a prefix
    in cases where the same model will be used to generate
    multiple of itself.
    """
    __prefix__ = None

    def _tablename(tablename, prefix):
        if prefix is None:
            return tablename.lower()
        else:
            return prefix.lower() + tablename.lower()

    @declared_attr
    def __tablename__(cls):
        return cls._tablename(cls.__name__, cls.__prefix__)


class Post(DynamicName, db.Model):
    __abstract__ = True
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(256))
    text = db.Column(db.Text)
    post_date = db.Column(db.DateTime)
    last_modified_date = db.Column(db.DateTime)

    # if 1 then make it a draft
    draft = db.Column(db.SmallInteger, default=0)

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


class Tag(DynamicName, db.Model):
    __abstract__ = True
    id = db.Column(db.Integer, primary_key=True)
    text = db.Column(db.String(128), unique=True, index=True)

    def __init__(self, text):
        self.text = text.upper()

class Tag_Posts(DynamicName, db.Model):
    __abstract__ = True
    __table_args__ = (
        db.PrimaryKeyConstraint('tag_id', 'post_id', name='uix_1'),)

    @declared_attr
    def tag_id(cls):
        key = cls._tablename('tag', cls.__prefix__) + '.id'
        return db.Column(db.Integer,
                         db.ForeignKey(key,
                                       onupdate="CASCADE",
                                       ondelete="CASCADE"), index=True)

    @declared_attr
    def post_id(cls):
        key = cls._tablename('post', cls.__prefix__) + '.id'
        return db.Column(db.Integer,
                         db.ForeignKey(key,
                                       onupdate="CASCADE",
                                       ondelete="CASCADE"), index=True)


class User_Posts(DynamicName, db.Model):
    __abstract__ = True
    __table_args__ = (
        db.PrimaryKeyConstraint('user_id', 'post_id', name='uix_2'),
    )

    user_id = db.Column(db.String(128), index=True)

    @declared_attr
    def post_id(cls):
        key = cls._tablename('post', cls.__prefix__) + '.id'
        return db.Column(db.Integer,
                         db.ForeignKey(key,
                                       onupdate="CASCADE",
                                       ondelete="CASCADE"), index=True)

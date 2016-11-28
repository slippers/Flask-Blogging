from .sqlastorage import SQLAStorage
from ..signals import sqla_initialized


class FSQLAStorage(SQLAStorage):
    """flask_sqlalchemy wrapper class for SQLAStorage
    """
    def __init__(self, db=None, prefix=None, bind_key=None):
        """
        :param db is flask_sqlalchemy SQLAlchemy object
        :param prefix: (Optional) is the prefix to the tablenames
        :param bind_key: (Optional) Reference the database to bind for multiple
        database scenario with binds.
        """
        if db is None:
            raise ValueError("db cannot be None.")

        engine = db.get_engine(db.get_app(), bind=bind_key)

        SQLAStorage.__init__(self,
                             engine=engine,
                             prefix=prefix,
                             bind_key=bind_key)

        db.metadata.reflect(engine)

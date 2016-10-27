from .sqlastorage import SQLAStorage
from ..signals import sqla_initialized
from .models import (
    Base
)


class FSQLAStorage(SQLAStorage):
    """flask_sqlalchemy wrapper class for SQLAStorage
    """
    def __init__(self, db=None, bind_key=None):
        """
        :param db is flask_sqlalchemy SQLAlchemy object
        :param bind_key: (Optional) Reference the database to bind for multiple
        database scenario with binds.
        :type bind: str
        """
        if db is None:
            raise ValueError("db cannot be None.")

        self._bind_key = bind_key

        # __bind_key__ is a custom attribute set in the model
        # it is used by wrapper extentions like flask-sqlalchemy and flask-alchy
        # to bind the model to a engine connection
        if bind_key:
            Base.__bind_key__ = bind_key

        engine = db.get_engine(db.get_app(), bind=bind_key)

        SQLAStorage.__init__(self, engine=engine)

        db.metadata.reflect(engine)

        self._initialized()

    def _initialized(self):
         sqla_initialized.send(self, engine=self._engine,
                              table_prefix=None,
                              meta=Base.metadata,
                              bind=self._bind_key)

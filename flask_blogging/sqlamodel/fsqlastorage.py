from .sqlastorage import SQLAStorage


class FSQLAStorage(SQLAStorage):

    def __init__(self, db=None, bind_key=None):

        if db is None:
            raise ValueError("db cannot be None.")

        engine = db.get_engine(db.get_app(), bind=bind_key)

        SQLAStorage.__init__(self, engine=engine, bind_key=bind_key)

        db.metadata.reflect(engine)

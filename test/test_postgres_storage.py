import unittest
from .storage import StorageTestMethods, StorageTestTables
try:
    from builtins import range
except ImportError:
    pass
from flask_blogging.sqlamodel import SQLAStorage, FSQLAStorage
from sqlalchemy import create_engine, MetaData
from flask_sqlalchemy import SQLAlchemy
import time
try:
    import psycopg2
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False


class StorageTest():

    def _create_storage(self):
        self.engine = create_engine(
            "postgresql+psycopg2://postgres:@localhost/flask_blogging")
        self.storage = SQLAStorage(self.engine)
        self.metadata = MetaData(bind=self.engine, reflect=True)

    def tearDown(self):
        self.storage.close()
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        metadata.drop_all(bind=self.engine)


@unittest.skipUnless(HAS_POSTGRES, "Requires psycopg2 Postgres package")
class TestPostgresStorageTables(StorageTest, StorageTestTables, unittest.TestCase):

    def dummy(self):
        pass

@unittest.skipUnless(HAS_POSTGRES, "Requires psycopg2 Postgres package")
class TestPostgresStorageMethods(StorageTest, StorageTestMethods, unittest.TestCase):

    def dummy(self):
        pass


@unittest.skipUnless(HAS_POSTGRES, "Requires psycopg2 Postgres package")
class TestPostgresBinds(StorageTestTables, unittest.TestCase):

    def _conn_string(self):
        return "postgresql+psycopg2://postgres:@localhost/flask_blogging"

    def _create_storage(self):
        conn_string = self._conn_string()
        self.app.config["SQLALCHEMY_BINDS"] = {
            'blog': conn_string
        }
        self._db = SQLAlchemy(self.app)
        self.storage = FSQLAStorage(self._db, bind_key='blog')
        self.metadata = self._db.metadata

    def tearDown(self):
        self.storage.close()
        self._db.drop_all(bind='blog')

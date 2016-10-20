import unittest
from .storage import StorageTestMethods, StorageTestTables
try:
    from builtins import range
except ImportError:
    pass
import tempfile
import os
from flask_blogging.sqlamodel import SQLAStorage
from sqlalchemy import create_engine, MetaData
from flask_sqlalchemy import SQLAlchemy
import time


class StorageTest():

    def _create_storage(self):
        temp_dir = tempfile.gettempdir()
        self._dbfile = os.path.join(temp_dir, "temp.db")
        self.engine = create_engine('sqlite:///'+self._dbfile)
        self.storage = SQLAStorage(self.engine)
        self.metadata = MetaData(bind=self.engine, reflect=True)

    def tearDown(self):
        os.remove(self._dbfile)


class TestSQLiteStorageTables(StorageTest, StorageTestTables, unittest.TestCase):

    def dummy(self):
        pass


class TestSQLiteStorageMethods(StorageTest, StorageTestMethods, unittest.TestCase):

    def dummy(self):
        pass


class TestSQLiteBinds(StorageTestTables, unittest.TestCase):

    def _conn_string(self, dbfile):
        return 'sqlite:///'+dbfile

    def _create_storage(self):
        temp_dir = tempfile.gettempdir()
        self._dbfile = os.path.join(temp_dir, "blog.db")
        conn_string = self._conn_string(self._dbfile)
        self.app.config["SQLALCHEMY_BINDS"] = {
            'blog': conn_string
        }
        self._db = SQLAlchemy(self.app)
        self._engine = self._db.get_engine(self.app, bind="blog")
        self.storage = SQLAStorage(self._engine, bind_key='blog')
        self.metadata = MetaData(bind=self._engine, reflect=True)

    def tearDown(self):
        os.remove(self._dbfile)

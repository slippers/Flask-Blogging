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
    import _mysql
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False


class StorageTest():

    def _create_storage(self):
        """
        mysql root user needs to have a blank password
        as this is not for production this is not an issue

        mysqladmin -uroot -proot password ''
        """
        self.engine = create_engine(
            "mysql+mysqldb://root@localhost/flask_blogging")
        self.storage = SQLAStorage(self.engine)
        self.metadata = MetaData(bind=self.engine, reflect=True)

    def tearDown(self):
        # myslq has issues with open transactions. calling session.close()
        self.storage.close()
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        metadata.drop_all(bind=self.engine)



@unittest.skipUnless(HAS_MYSQL, "Package mysql-python needs to be install to "
                                "run this test.")
class TestMySQLStorageTables(StorageTest, StorageTestTables, unittest.TestCase):

    def dummy(self):
        pass

@unittest.skipUnless(HAS_MYSQL, "Package mysql-python needs to be install to "
                                "run this test.")
class TestMySQLStorageMethods(StorageTest, StorageTestMethods, unittest.TestCase):

    def dummy(self):
        pass



@unittest.skipUnless(HAS_MYSQL, "Package mysql-python needs to be install to "
                                "run this test.")
class TestMySQLBinds(StorageTestTables, unittest.TestCase):

    def _conn_string(self):
        return "mysql+mysqldb://root:@localhost/flask_blogging"

    def _create_storage(self):
        conn_string = self._conn_string()
        self.app.config["SQLALCHEMY_BINDS"] = {
            'blog': conn_string
        }
        self._db = SQLAlchemy(self.app)
        self.storage = FSQLAStorage(self._db, bind_key='blog')
        self.metadata = self._db.metadata

    def tearDown(self):
        self._db.drop_all(bind='blog')

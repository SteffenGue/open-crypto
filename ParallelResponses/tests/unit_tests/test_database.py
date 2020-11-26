import unittest
from sqlalchemy import create_engine, MetaData, or_, and_, tuple_, inspect
from sqlalchemy.exc import ProgrammingError, OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, Query, aliased, joinedload
from sqlalchemy_utils import database_exists, create_database
from pandas import read_sql_query as pd_read_sql_query
#from model.database.db_handler import DatabaseHandler
from model.database.tables import metadata


class TestDatabase(unittest.TestCase):
    """
    Test module for testing db_handler class

    Authors:


    Since:
        23.11.2020

    Version:

    """

    def setUp(self):
        """
        Setting up the in memory test database, with give metadata.
        """
        self.engine = create_engine('sqlite:///:memory:')
        #Base = declarative_base()  # pylint: disable=invalid-name
        #self.metadata = Base.metadata
        self.metadata = metadata
        self.metadata.create_all(self.engine)
        self.sessionFactory = sessionmaker(bind=self.engine)

    def tearDown(self):
        """
        Delete / drop all tables from the database.
        """
        self.metadata.drop_all(self.engine)

    def test(self):
        session = self.sessionFactory()
        #result = session.query().all()
        self.assertEqual(1, 1)


if __name__ == "__main__":
    unittest.main()

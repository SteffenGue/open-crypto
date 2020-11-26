import unittest
from sqlalchemy import create_engine, MetaData, or_, and_, tuple_, inspect
from sqlalchemy.exc import ProgrammingError, OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, Query, aliased
from sqlalchemy_utils import database_exists, create_database
from pandas import read_sql_query as pd_read_sql_query
from ParallelResponses.model.database.db_handler import DatabaseHandler
from ParallelResponses.model.database.tables import metadata


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
        self.metadata = metadata
        self.metadata.create_all(self.engine)
        self.sessionFactory = sessionmaker(bind=self.engine)

#    def tearDown(self):
#        """
#        Delete / drop all tables from the database.
#        """
#        self.metadata.drop_all(self.engine)

    def test_get_all_currency_pairs_from_exchange(self):
        """

        """
        session = self.sessionFactory()
        self.assertTrue(True)
        session.close()

    def test_get_currency_pairs_with_first_currency(self):
        """

        """
        session = self.sessionFactory()
        self.assertTrue(True)
        session.close()

    def test_get_currency_pairs_with_second_currency(self):
        """

        """
        session = self.sessionFactory()
        self.assertTrue(True)
        session.close()

    def test_get_currency_pairs(self):
        """

        """
        session = self.sessionFactory()
        self.assertTrue(True)
        session.close()

    def test_get_exchanges_currency_pairs(self):
        """

        """
        session = self.sessionFactory()
        self.assertTrue(True)
        session.close()

    def test_persist_and_get_exchange_id(self):
        """

        """
        session = self.sessionFactory()
        # result = session.query().all()
        self.assertTrue(True)
        session.close()

    def test_persist_and_get_exchange_currency_pair(self):
        """

        """
        session = self.sessionFactory()
        self.assertTrue(True)
        session.close()

    def test_persist_response(self):
        """

        """
        session = self.sessionFactory()
        self.assertTrue(True)
        session.close()

    def test_get_readable_query(self):
        """

        """
        session = self.sessionFactory()
        self.assertTrue(True)
        session.close()


if __name__ == "__main__":
    unittest.main()

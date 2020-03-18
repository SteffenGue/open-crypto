from datetime import datetime
from typing import Sequence, List, Tuple, Any, Iterator
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, MetaData, or_, and_, exists
from sqlalchemy.orm import sessionmaker, Session
from tables import Currency, Exchange, ExchangeCurrencyPairs, Ticker
import time


class DatabaseHandler:
    """
    Class which handles every interaction with the database.
    This includes most of the time checking if values exist in
    the database or storing/querying values.

    For querying and storing values the library sqlalchemy is used.

    Attributes:
        sessionFactory: sessionmaker
           Factory for connections to the database.
    """
    sessionFactory: sessionmaker

    def __init__(self,
                 metadata: MetaData,
                 sqltype: str,
                 client: str,
                 user_name: str,
                 password: str,
                 host: str,
                 port: str,
                 db_name: str):
        """
        Initializes the database-handler.

        Builds the connection-string and tries to connect to the database.

        Creates with the given metadata tables which do not already exist.
        Won't make a new table if the name already exists,
        so changes to the table-structure have to be made by hand in the database
        or the table has to be deleted.

        Initializes the sessionFactory with the created engine.
        Engine variable is no attribute and currently only exists in the constructor.

        :param metadata: Metadata
            Information about the table-structure of the database.
            See tables.py for more information.
        :param sqltype: atr
            Type of the database sql-dialect. ('postgresql' for us)
        :param client: str
            Name of the Client which is used to connect to the database.
        :param user_name: str
            Username under which this program connects to the database.
        :param password: str
            Password for this username.
        :param host: str
            Hostname or Hostaddress from the database.
        :param port: str
            Connection-Port (usually 5432 for Postgres)
        :param db_name: str
            Name of the database.
        """

        engine = create_engine('{}+{}://{}:{}@{}:{}/{}'.format(sqltype, client, user_name, password, host, port, db_name))

        if not database_exists(engine.url):
            create_database(engine.url)
            print("Database created")
        metadata.create_all(engine)
        self.sessionFactory = sessionmaker(bind=engine)


    #TODO: Load all DB-Entries once in the beginning instead of querying every item speratly?!
    #TODO: session als parameter entfernt
    #TODO: wird momentan nicht genutzt
    def get_or_create_DB_entry(self,
                               ticker: Tuple[str, datetime, datetime, str, str, float, float, float, float, float]):
        """
        This function queries or creates the corresponding database entries. If the ExchangeCurrencyPair already
          exists, the object is queries and appended to the ticker tuple. If one or more of the entries do not
          exist, the single object (e.g. Currency or Exchange Object) is first created and then an
          ExchangeCurrencyPair-Object is created. The missing entry is automatically persisted in the DB.
        It is necessary to distinguish between existing and not existing DB-entities when creating the
          ExchangeCurrencyPair-Object. Otherwise an Unique-Constraint Error is raised.

        :param session: SQL-Alchemy Session
            The running session from 'DatabaseHandler.persist_tickers'
        :param ticker: Tuple
            The ticker Tuple from 'DatabaseHandler.persist_tickers'
        :return: ticker_update: Tuple
            An Tuple including an ORM-Query Object (ExchangeCurrencyPairs-Object) on indices 0
        """
        session = self.sessionFactory()
        exchange = session.query(Exchange).filter(Exchange.name == ticker[0]).first()
        first = session.query(Currency).filter(Currency.name == ticker[3]).first()
        second = session.query(Currency).filter(Currency.name == ticker[4]).first()

        try:
            if exchange != None and first != None and second != None:

                exchange_pair = session.query(ExchangeCurrencyPairs).filter(ExchangeCurrencyPairs.exchange_id == exchange.id,
                                                                            ExchangeCurrencyPairs.first_id == first.id,
                                                                            ExchangeCurrencyPairs.second_id == second.id).first()

            else:
                if exchange == None:
                    exchange = Exchange(name=ticker[0])
                if first == None:
                    first = Currency(name=ticker[3])
                if second == None:
                    second = Currency(name=ticker[4])

                exchange_pair = ExchangeCurrencyPairs(exchange=exchange,
                                                      first=first,
                                                      second=second)

            if exchange_pair == None:
                exchange_pair = ExchangeCurrencyPairs(exchange=exchange,
                                                      first=first,
                                                      second=second)
            session.close()
        except Exception as e:
            print(e, e.__cause__)
            session.rollback()
            pass

        ticker_update = list(ticker)
        ticker_update.insert(0, exchange_pair)

        return tuple(ticker_update)


    def persist_tickers(self, tickers: Iterator[Tuple[str, datetime, datetime,  str, str, float, float, float, float, float]]):
        """
        Persists the given tuples of ticker-data.
        TUPLES MUST HAVE THE DESCRIBED STRUCTURE STATED BELOW

        The method checks for each tuple if the referenced exchange and
        currencies exist in the database.
        If so, the Method creates with the stored data of the current tuple
        a new Ticker-object which is then added to the commit.
        After all tuples where checked, the added Ticker-objects will be
        committed and the connection will be closed.

        Exceptions will be caught but not really handled.
        TODO: Exception handling and
        TODO: Logging of Exception

        :param tickers: Iterator
            Iterator of tuples containing ticker-data.
            Tuple must have the following structure:
                (exchange-name,
                 start_time,
                 response_time,
                 first_currency_symbol,
                 second_currency_symbol,
                 ticker_last_price,
                 ticker_last_trade,
                 ticker_best_ask,
                 ticker_best_bid,
                 ticker_daily_volume)
        """
        session = self.sessionFactory()
        try:
            for ticker in tickers:
                        ticker_append = DatabaseHandler.get_or_create_DB_entry(self, ticker)

                        ticker_tuple = Ticker(exchange_pair=ticker_append[0],
                                              start_time=ticker_append[2],
                                              response_time=ticker_append[3],
                                              last_price=ticker_append[6],
                                              last_trade=ticker_append[7],
                                              best_ask=ticker_append[8],
                                              best_bid=ticker_append[9],
                                              daily_volume=ticker_append[10])

                        session.add(ticker_tuple)

            session.commit()
            session.close()

        except Exception as e:
            print(e, e.__cause__)
            session.rollback()
            pass


    #TODO diskutabel, möglichkeit für queries kann nach außen gegeben werden -> utilities
    #Antwort: kommt weg
    def get_session(self):
        """
        Getter method for the given instance of the session.
        :return session: the instance of the session
        """
        session = self.sessionFactory()
        return session

    def check_exceptions(self, exceptions: dict):
        """
        This method will call a classmethod of the Exchange class to update the flags for the given exchanges.
        :param exceptions: the dictionary which contains the explicit exchanges which have thrown an exception

        self.sessionFactory gives the instance of the current session.
        """
        Exchange.update_exceptions(self.sessionFactory(), exceptions)

    #TODO: Dokumentation
    def get_exchange_currency_pairs(self, exchange_name: str) -> List[ExchangeCurrencyPairs]:
        session = self.sessionFactory()
        currency_pairs = list()
        exchange = session.query(Exchange).filter(Exchange.name.__eq__(exchange_name)).first()
        if exchange:
            currency_pairs = session.query(ExchangeCurrencyPairs).filter(ExchangeCurrencyPairs.exchange_id.__eq__(exchange.id)).all()
        session.close()
        return currency_pairs
        

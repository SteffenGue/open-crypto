from datetime import datetime
from typing import Sequence, List, Tuple, Any, Iterator
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, MetaData, or_, and_, exists
from sqlalchemy.orm import sessionmaker, Session
from tables import Currency, Exchange, ExchangeCurrencyPair, Ticker


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

        engine = create_engine(
            '{}+{}://{}:{}@{}:{}/{}'.format(sqltype, client, user_name, password, host, port, db_name))

        if not database_exists(engine.url):
            create_database(engine.url)
            print(f"Database '{db_name}' created")
        metadata.create_all(engine)
        self.sessionFactory = sessionmaker(bind=engine)

    # ToDo: Load all DB-Entries once in the beginning instead of querying every item speratly?!



    def get_or_create_DB_entry(self,
                               session: Session,
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
            An Tuple including an ORM-Query Object (ExchangeCurrencyPair-Object) on indices 0
        """

        exchange = session.query(Exchange).filter(Exchange.name == ticker[0]).first()
        first = session.query(Currency).filter(Currency.name == ticker[3]).first()
        second = session.query(Currency).filter(Currency.name == ticker[4]).first()

        try:
            if exchange != None and first != None and second != None:

                exchange_pair = session.query(ExchangeCurrencyPair).filter(
                    ExchangeCurrencyPair.exchange_id == exchange.id,
                    ExchangeCurrencyPair.first_id == first.id,
                    ExchangeCurrencyPair.second_id == second.id).first()

            else:
                if exchange is None:
                    exchange = Exchange(name=ticker[0])
                if first is None:
                    first = Currency(name=ticker[3])
                if second is None:
                    second = Currency(name=ticker[4])

                exchange_pair = ExchangeCurrencyPair(exchange=exchange,
                                                     first=first,
                                                     second=second)

            if exchange_pair is None:
                exchange_pair = ExchangeCurrencyPair(exchange=exchange,
                                                     first=first,
                                                     second=second)
        except Exception as e:
            print(e, e.__cause__)
            session.rollback()
            pass

        ticker_update = list(ticker)
        ticker_update.insert(0, exchange_pair)

        return tuple(ticker_update)

    def persist_tickers(self,
                        tickers: Iterator[Tuple[str, datetime, datetime, str, str, float, float, float, float, float]]):
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
                ticker_append = DatabaseHandler.get_or_create_DB_entry(self, session, ticker)

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
        finally:
            session.close()

    #todo: methode in utilities ändern auf abghängigkeit der flag variable und nicht database eintrag
    def get_active_exchanges(self):
        """
        Query every inactive exchange from the database
        :return: list of all inactive exchanges
        """

        session = self.sessionFactory()
        query = set(session.query(Exchange.name).filter(Exchange.active == False).all())
        session.close()
        return query

    #todo: teilweises auslagern in utilities
    #es bleibt nur die funktion den wert active zu setzen
    def update_exceptions(self, exchanges_r):
        """
        Method to update the exception_counter. If An exception occurred add 1 to the counter,
            else set back to zero.
        Further exchanges with a total of 3 exceptions in a row will be set inactive.

        :param exchanges_r: list
            List of the real instances of the exchanges.
        :return: None
        """
        session = self.sessionFactory()
        exchanges_db = list(session.query(Exchange).all())
        exceptions = {}
        for exchange in exchanges_r:
            if not exchanges_r[exchange].active_flag:
                exceptions['{}'.format(exchange.name)] = 1
        for exchange in exchanges_db:
            if exchange.name in exceptions:
                exchange.active = False
                print('{} was set inactive in the db.'.format(exchange.name))

        try:
            session.commit()

        except Exception as e:
            print(e, e.__cause__)
            session.rollback()
            pass

        finally:
            session.close()

    def request_params(self, function, exchange, *args: list):
        """
        Helper-method to perform function calls from request parameters. If one needs a variable
        request parameter (for example: the latest entry of a specific currency pair in the database),
        a lambda function can be defined in utilities.py and is called in exchanges.py as the parameters
        from the yaml-files are read out. The specifications when a function call takes place are described
        in exchanges.py under the method Exchanges.extract_request_urls().

        :param function: dict
            {'function': <function>, 'params': int, 'session': boolean}

        :param exchange: str
            the actual exchange name

        :param args: *args - variable number of parameters according to the function

        :return the desired unassigned value. Can be string/float/integer.
        """

        if function['params'] == len(args):
            session = self.sessionFactory()

            func = function['function']

            if function['session'] is True:
                # evaluate the *args to rewrite them as the objects they should represent.
                args = (session, eval(*args))

            session.close()
            return func(*args).first()

        else:
            raise KeyError(f'Wrong number of arguments for {exchange} - {function["name"]}. '
                           f'Expected {function["params"]} - got {len(args)}')

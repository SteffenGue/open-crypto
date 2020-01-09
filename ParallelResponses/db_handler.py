from datetime import datetime
from typing import Sequence, List, Tuple, Any, Iterator

from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, MetaData, or_, and_
from sqlalchemy.orm import sessionmaker, Session
from tables import Currency, Exchange, Ticker


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

    def persist_currencies(self, currencies: Sequence[Tuple]):
        """
        Persists the given Sequence of currency-tuples which should
        contain viable information.
        If the information is viable is not tested.
        TODO: make more robust -> test content of tuples
        TODO: Exception handling and logging

        The method creates for each tuple a new currency-object
        and tests if there is already an entry with the given
        name and symbol.
        If not the currency-object is added to the commit.
        After all currency-objects where tested, the ones
        who are not already stored in the database will be committed and
        the connection will be closed.

        :param currencies: Sequence[tuple]
            List of tuples which should be persisted.
            Tuple should must have the following structure:
                    (Name, Symbol)
        """
        session = self.sessionFactory()
        generate_pairs = list()

        for curr in currencies:
            curr_name = curr[0]
            curr_symbol = curr[1]
            currency = Currency(name=curr_name, symbol=curr_symbol)
            # ask if currency existsts in database
            if session.query(Currency). \
                    filter(Currency.name == currency.name). \
                    filter(Currency.symbol == currency.symbol).first() is None:
                session.add(currency)
        session.commit()
        session.close()

    def get_all_currency_ids(self) -> List[int]:
        """
        Queries all ids(integer) in the Currency-Table that exist in the database.

        TODO: Exception handling and logging

        :return: List[int] of all ids.
        """
        session = self.sessionFactory()
        tuple_ids = session.query(Currency.id).all()
        session.close()
        list_ids = [value for value, in tuple_ids]
        return list_ids

    def persist_tickers(self, tickers: Iterator[Tuple[str, datetime, str, str, float, float, float, float, float]]):
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
        TODO: Exception handling and logging

        :param tickers: Iterator
            Iterator of tuples containing ticker-data.
            Tuple must have the following structure:
                (exchange-name,
                 timestamp,
                 first_currency_symbol,
                 second_currency_symbol,
                 ticker_last_price,
                 ticker_last_trade,
                 ticker_best_ask,
                 ticker_best_bid,
                 ticker_daily_volume)
        """
        session = self.sessionFactory()
        for ticker in tickers:
            #if ticker[2] and ticker[3] and self.currency_exists(ticker[2]) and self.currency_exists(ticker[3]):
                ticker_tuple = Ticker(exchange=ticker[0],
                                      time=ticker[1],
                                      first_currency=ticker[2],
                                      second_currency=ticker[3],
                                      last_price=ticker[4],
                                      last_trade=ticker[5],
                                      best_ask=ticker[6],
                                      best_bid=ticker[7],
                                      daily_volume=ticker[8])
                session.add(ticker_tuple)
           # else:
             #   print('Currency {} oder {} nicht in der Datenbank gefunden.'.format(ticker[2], ticker[3]))
        try:
            session.commit()
        except Exception:
            print('Exception beim persistieren.')

        session.close()

    def get_exchange_id(self, exchange_name: str) -> int:
        """
        Searches in the database for the given name and returns
        the corresponding id.

        :param exchange_name: str
            Name of the searched exchange.

        :return: int
            Id of the searched exchange.
            Is None if no exchange with the given name was found in the database.
        """
        session = self.sessionFactory()
        result = session.query(Exchange.id). \
            filter(Exchange.name == exchange_name).first()
        session.close()
        return result

    def persist_exchanges(self, exchanges: List[str]):
        """
        Persists the given names of exchanges in the database.

        Method only persists exchange-names which are not already
        stored in the database.

        TODO: Exception handling and logging

        :param exchanges: List[str]
            List of exchange_names which should be persisted.
        """
        session = self.sessionFactory()
        for exchange_name in exchanges:
            if session.query(Exchange.name). \
                    filter(Exchange.name == exchange_name). \
                    first() is None:
                session.add(Exchange(name=exchange_name))
        session.commit()
        session.close()

    def currency_exists(self, curr_symbol: str) -> bool:
        session = self.sessionFactory()

        result = session.query(Currency). \
            filter(Currency.symbol == curr_symbol.upper()). \
            first()
        session.close()
        return result is not None

# <--------------------- Currency-Pair Methods (currently no use) --------------------->
# def get_currency_pair_id(self, first_currency_name: str, second_currency_name: str) -> int:
#     session = self.sessionFactory()
#
#     first_currency_id = session.query(Currency.id). \
#         filter(Currency.symbol == first_currency_name.upper()). \
#         first()
#
#     second_currency_id = session.query(Currency.id). \
#         filter(Currency.symbol == second_currency_name.upper()). \
#         first()
#
#     if first_currency_id is not None and second_currency_id is not None:
#         # Value aus Tupel holen
#         first_currency_id = first_currency_id[0]
#         second_currency_id = second_currency_id[0]
#
#         result = session.query(CurrencyPair.id). \
#             filter(or_(and_(CurrencyPair.first_id == first_currency_id,
#                             CurrencyPair.second_id == second_currency_id),
#                        and_(CurrencyPair.first_id == second_currency_id,
#                             CurrencyPair.second_id == first_currency_id))). \
#             first()
#     else:
#         result = None
#
#     session.close()
#
#     # Value aus Tuple holen, der returnt wird
#     if result is not None:
#         result = result[0]
#     return result

# def generate_currency_pairs(self, ids: Sequence[int]):
#     pairs = list()
#
#     persisted_ids = self.get_all_currency_ids()
#     for id in ids:
#         for p_id in persisted_ids:
#             if not id == p_id:
#                 pairs.append((id, p_id))
#                 print('{}, {}'.format(id, p_id))
#
#     return pairs

# def bulk_currency_pairs(self, pairs: Sequence[tuple]):
#     currency_pairs = list()
#     for p in pairs:
#         currency_pairs.append(CurrencyPair(first_id=p[0], second_id=p[1]))
#
#     session = self.sessionFactory()
#     session.bulk_save_objects(currency_pairs)
#     session.commit()
#     print('done')
#     session.close()

# def persist_currency_pairs(self, currency_pairs: Sequence[tuple]):
#     '''
#     :param currency_pairs: Sequen
#     '''
#     session = self.sessionFactory()
#
#     to_add = len(currency_pairs)
#     counter = 0
#
#     for pair in currency_pairs:
#         counter = counter + 1
#         currency_pair = CurrencyPair(first_id=pair[0], second_id=pair[1])
#         if session.query(CurrencyPair). \
#                 filter(or_(and_(CurrencyPair.first_id == currency_pair.first_id,
#                                 CurrencyPair.second_id == currency_pair.second_id),
#                            and_(CurrencyPair.first_id == currency_pair.second_id,
#                                 CurrencyPair.second_id == currency_pair.first_id))).first() is None:
#             session.add(currency_pair)
#             print('Paar: {}, {} hinzugefügt.'.format(currency_pair.first_id, currency_pair.second_id))
#
#         print('{} von {}'.format(counter, to_add))
#     session.commit()
#     print('index ist lit')
#     session.close()

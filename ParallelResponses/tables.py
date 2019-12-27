from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()  # pylint: disable=invalid-name
metadata = Base.metadata


class Currency(Base):
    """
    Database ORM-Class storing all currencies.

    id: int
        Autoincremented unique identifier.
    name: str
        Name of the currency written out.
    symbol: str
        Abbreviation of the currency
    """

    __tablename__ = 'currencies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True, nullable=False)
    symbol = Column(String(10), nullable=False)


class Exchange(Base):
    """
    Database ORM-Class storing the exchanges table. ALl exchanges used to perform requests
    are listed in this table.

    id: int
        Autoincremented unique identifier.
    name: str
        The explicit name of the exchange defined in the .yaml-file.
    """

    __tablename__ = 'exchanges'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False, unique=True)


# TODO Macht überhaupt Sinn?
class ExchangeCurrencyPair(Base):
    """
    Database ORM-Class storing the ExchangeCurrencyPairs.

    exchange_id: int
        The unique id of each exchange taken from the ForeignKey.
    currency_first_id: int
        The unique id of each currency_pair taken from the table Currency.
    currency_first_id: int
        The unique id of each currency_pair taken from the table Currency.
    __table_args__:
        First ID must be unequal to Second ID.
    """

    __tablename__ = 'exchange_currency_pairs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, ForeignKey(Exchange.id), primary_key=True, nullable=False)
    currency_first_id = Column(Integer, ForeignKey(Currency.id), primary_key=True, nullable=False)
    currency_second_id = Column(Integer, ForeignKey(Currency.id), primary_key=True, nullable=False)

    #__table_args__ = (CheckConstraint(currency_first_id != currency_second_id))


class Ticker(Base):
    """
    TODO: Update if no longer correct after database sturcture is updated (Issue #4, 03.12.2019).
    Database ORM-Class storing the ticker data.

    ECP_ID: int
        Autoincremented unique Exchange_Currency_Pair identifier. The ECP_ID is used by the database_handler to check
        for existing exchange_currency_pairs. If not existing, currency_pair (incl. the currencies) are created.
    start_time: datetime
        Unified timestamp for each exchange in a request run.
    time: datetime
        Timestamp of the response. Timestamps are created by the os, the delivered ones from the exchanges are not used.
        Timestamps are equal for each execution (resulting in an error of max. 5 seconds) to ease data usage later.
    last_price: float
        Latest price of the currency_pair given from the exchange.
    last_trade: float
        Information on the last trade of a currency_pair. Information can differ for each exchange!
    best_ask: float
        Best ask price of an exchange for a currency_pair.
    best_bid: float
        Best bid price of an exchange for a currency_pair.
    daily_volume: float
        The traded volume of an currency_pair on an exchange. Definition can differ for each exchange!

    TODO: Describe __table_args__ as soon as the database structure is defined.
    __table_args__ = ??
    """

    __tablename__ = "tickers"

    exchange = Column(String, ForeignKey(Exchange.name), primary_key=True)
    first_currency = Column(String,
                            primary_key=True)
    second_currency = Column(String,
                             primary_key=True)
    start_time = Column(DateTime)
    time = Column(DateTime, primary_key=True)
    # last_price = Column(Float, CheckConstraint("last_price > 0"))
    # last_trade = Column(Float, CheckConstraint("last_trade > 0"))
    # best_ask = Column(Float, CheckConstraint("best_ask >= 0"))
    # best_bid = Column(Float, CheckConstraint("best_bid >= 0"))
    # daily_volume = Column(Float, CheckConstraint("daily_volume >= 0"))
    last_price = Column(Float)
    last_trade = Column(Float)
    best_ask = Column(Float)
    best_bid = Column(Float)
    daily_volume = Column(Float)
    # __table_args__ = (ForeignKeyConstraint(exchange, Exchange.id),
    #                   ForeignKeyConstraint(currency_pair_id, CurrencyPair.id))



# <----------------------Currency-Pair (currently no use)-------------------------------->
"""
This table is no longer in use as is slows down the process significantly. 
Information about the CurrencyPairs are only stored in ExchangeCurrencyPair.
"""
# class CurrencyPair(Base):
#     """
#     Database ORM-Class storing all currency_pairs. Currency_pairs are auto updates/created
#     depending on the single API-responses.
#
#     id: int
#         Autoincremented unique identifier
#     first_id: int
#         Teflecting the first currency of the pair. The ID is fetched from the table Currency.
#         The real name of the currency is stored in the table Currency
#     second_id: int
#         Teflecting the second currency of the pair. The ID is fetch from the table Currency.
#         The real name of the currency is stored in the table Currency
#     """
#
#     __tablename__ = 'currency_pairs'
#
#     id = Column(Integer, primary_key=True, autoincrement=True, index=True)
#     first_id = Column(Integer, ForeignKey(Currency.id), nullable=False)
#     second_id = Column(Integer, ForeignKey(Currency.id), nullable=False)

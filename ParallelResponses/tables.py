from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()  # pylint: disable=invalid-name
metadata = Base.metadata


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
    #symbol = Column(String(10), nullable=False)


class ExchangeCurrencyPairs(Base):
    """
    Database ORM-Class storing the ExchangeCurrencyPairs.

    exchange_id: int
        The unique id of each exchange taken from the ForeignKey.
    first_id: int
        The unique id of each currency_pair taken from the table Currency.
    second_id: int
        The unique id of each currency_pair taken from the table Currency.

    exchange: relationship
        Relationship with table Exchange
    first: relationship
        Relationship with table Currency
    second: relationship
        Relationship with table Currency
    __table_args__:
        First ID must be unequal to Second ID.
    """

    __tablename__ = 'exchanges_currency_pairs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'))
    first_id = Column(Integer, ForeignKey('currencies.id'))
    second_id = Column(Integer, ForeignKey('currencies.id'))

    exchange = relationship("Exchange", backref="exchanges_currency_pairs")
    first = relationship("Currency", foreign_keys="ExchangeCurrencyPairs.first_id")
    second = relationship("Currency", foreign_keys="ExchangeCurrencyPairs.second_id")

    __table_args__ = (CheckConstraint(first_id != second_id),)


class Ticker(Base):
    """
    TODO: Update if no longer correct after database sturcture is updated (Issue #4, 03.12.2019).
    Database ORM-Class storing the ticker data.

    exchange_pair_id: int
        Unique Exchange_Currency_Pair identifier. The exchange_pair_id is used by the database_handler to check
         for existing exchange_currency_pairs. If not existing, the currency and/or exchange is created.

    exchange_pair: relationship
        The corresponding relationship table with ExchangeCurrencyPairs

    start_time: DateTime
        Timestamp of the execution of an exchange request (UTC). Timestamps are unique for each exchange.

    response_time: DateTime
        Timestamp of the response. Timestamps are created by the OS, the delivered ones from the exchanges are not used.
        Timestamps are equal for each exchange for one run (resulting in an error of approx. 5 seconds average)
         to ease data usage later.
        Timestamps are rounded to seconds (UTC)

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

   # entry_id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_pair_id = Column(Integer, ForeignKey('exchanges_currency_pairs.id'), primary_key=True)

   # exchange_pair = relationship('ExchangeCurrencyPairs', foreign_keys="Ticker.exchange_pair_id")
    exchange_pair = relationship('ExchangeCurrencyPairs', backref="tickers")

    # exchange = Column(String, ForeignKey(Exchange.name), primary_key=True)
    # first = Column(String,
    #                         primary_key=True)
    # second = Column(String,
    #                          primary_key=True)
    response_time = Column(DateTime, primary_key=True)
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



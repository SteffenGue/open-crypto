import sqlalchemy
from sqlalchemy import join, Column, Integer, String, Boolean, ForeignKey, CheckConstraint, Float, DateTime, select, \
    Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, validates, aliased, mapper
from sqlalchemy_utils import create_view
from sqlalchemy_utils.functions import orm

Base = declarative_base()  # pylint: disable=invalid-name
metadata = Base.metadata


# class BaseMixin(object):
#     """
#     Classmethods blueprint for the database classes. Class "BaseMixin" needs to be inherited to an Object.
#     """
#
#     @classmethod
#     def method_xyz(cls):
#         pass


class Exchange(Base):
    """
    Database ORM-Class storing the exchange table. ALl exchange used to perform requests
    are listed in this table.

    id: int
        Autoincremented unique identifier.
    name: str
        The explicit name of the exchange defined in the .yaml-file.
    """

    __tablename__ = 'exchanges'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False, unique=True, )
    active = Column(Boolean, default=True)
    exceptions = Column(Integer, unique=False, nullable=True, default=0)
    total_exceptions = Column(Integer, unique=False, nullable=True, default=0)

    def __repr__(self):
        return "#{}: {}, Active: {}".format(self.id, self.name, self.active)

    @validates('name')
    def convert_upper(self, key, value):
        return value.upper()


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

    def __repr__(self):
        return "#{}: {}".format(self.id, self.name)

    @validates('name')
    def convert_upper(self, key, value):
        return value.upper()


class ExchangeCurrencyPair(Base):
    """
    Database ORM-Class storing the ExchangeCurrencyPair.

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

    exchange = relationship("Exchange", backref="exchanges_currency_pairs", lazy='joined')
    first = relationship("Currency", foreign_keys="ExchangeCurrencyPair.first_id", lazy='joined')
    second = relationship("Currency", foreign_keys="ExchangeCurrencyPair.second_id", lazy='joined')

    __table_args__ = (CheckConstraint(first_id != second_id),)

    def __repr__(self):
        return "#{}: {}({}), {}({})-{}({})".format(self.id,
                                                   self.exchange.name, self.exchange_id,
                                                   self.first.name, self.first_id,
                                                   self.second.name, self.second_id)

    def __str__(self):
        return "#{}: {}({}), {}({})-{}({})".format(self.id,
                                                   self.exchange.name, self.exchange_id,
                                                   self.first.name, self.first_id,
                                                   self.second.name, self.second_id)


class Ticker(Base):
    """
    Database ORM-Class storing the ticker data.

    exchange_pair_id: int
        Unique Exchange_Currency_Pair identifier. The exchange_pair_id is used by the database_handler to check
         for existing exchange_currency_pairs. If not existing, the currency and/or exchange is created.

    exchange_pair: relationship
        The corresponding relationship table with ExchangeCurrencyPair

    start_time: DateTime
        Timestamp of the execution of an exchange request (UTC). Timestamps are unique for each exchange.

    response_time: DateTime
        Timestamp of the response. Timestamps are created by the OS, the delivered ones from the exchange are not used.
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

    exchange_pair_id = Column(Integer, ForeignKey('exchanges_currency_pairs.id'), primary_key=True)
    exchange_pair = relationship('ExchangeCurrencyPair', backref="tickers")

    start_time = Column(DateTime)
    response_time = Column(DateTime, primary_key=True)
    last_price = Column(Float)
    last_trade = Column(Float)
    best_ask = Column(Float)
    best_bid = Column(Float)
    daily_volume = Column(Float)

    def __repr__(self):
        return "#{}, {}: {}-{}, ${} at {}".format(self.exchange_pair_id,
                                                  self.exchange_pair.exchange.name,
                                                  self.exchange_pair.first.name,
                                                  self.exchange_pair.second.name,
                                                  self.last_price,
                                                  self.start_time)


class HistoricRate(Base):
    """
    Table for the method historic_rates. Tables contains the exchange_currency_pair_id, gathered from the
    foreign_keys.

    Primary_keys are Exchange_Pair_id and the timestamp.

    Table contains standard OHLCV values (Open - High - Low - Close - Volume24h)

    __repr__(self) describes the representations of the table if queried. The database will return the
    object as normal, but print "ID, Exchange: First-Second, $ Close at time" in clear names for better
    readability.
    """

    __tablename__ = 'historic_rates'

    exchange_pair_id = Column(Integer, ForeignKey('exchanges_currency_pairs.id'), primary_key=True)
    exchange_pair = relationship('ExchangeCurrencyPair', backref="historic_rates")
    timestamp = Column(DateTime, primary_key=True)

    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

    def __repr__(self):
        return "ID {}, {}: {}-{}, close {} at {}".format(self.exchange_pair_id,
                                                         self.exchange_pair.exchange.name,
                                                         self.exchange_pair.first.name,
                                                         self.exchange_pair.second.name,
                                                         self.close,
                                                         self.timestamp)


class Trade(Base):
    """
    Table for the method trades. Tables contains the exchange_currency_pair_id, gathered from the
    foreign_keys.
    Primary_keys are Exchange_Pair_id and the timestamp.

    Table contains the last trades, trade amount, trade direction (buy/sell) and timestamp.

    __repr__(self) describes the representation if queried.

    """
    __tablename__ = 'trades'

    exchange_pair_id = Column(Integer, ForeignKey('exchanges_currency_pairs.id'), primary_key=True)
    exchange_pair = relationship('ExchangeCurrencyPair', backref="trades")
    timestamp = Column(DateTime, primary_key=True)

    amount = Column(Float)
    best_bid = Column(Float)
    best_ask = Column(Float)
    price = Column(Float)
    direction = Column(String)

    def __repr__(self):
        return "Last Transction: {}, {}-{}: {} for {} at {}".format(self.exchange_pair.exchange.name,
                                                                    self.exchange_pair.first.name,
                                                                    self.exchange_pair.second.name,
                                                                    self.amount,
                                                                    self.price,
                                                                    self.timestamp)

    @validates('direction')
    def convert_upper(self, key, value):
        return value.upper()


class ExchangeCurrencyPairView(Base):
    """
    View vor ExchangeCurrencyPairs.
    """
    first = aliased(Currency)
    second = aliased(Currency)

    __table__ = create_view(
        name='exchanges_currency_pairs_view',
        selectable=sqlalchemy.select(
            [
                ExchangeCurrencyPair.id,
                Exchange.name.label('exchange_name'),
                first.name.label('first_name'),
                second.name.label('second_name')
            ],
            from_obj=(
                ExchangeCurrencyPair.__table__.join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id)
                    .join(first, ExchangeCurrencyPair.first_id == first.id)
                    .join(second, ExchangeCurrencyPair.second_id == second.id)
            )
        ),
        metadata=Base.metadata
    )


class TickersView(Base):
    """
    View for Tickers.
    Instead of only showing the ID of the ExchangeCurrencyPair the View displays
    the exchange name and the name of the first and second currency.
    """
    first = aliased(Currency)
    second = aliased(Currency)
    __table__ = create_view(
        name='tickers_view',
        selectable=select(
            [
                Exchange.name.label('exchange'),
                first.name.label('first_currency'),
                second.name.label('second_currency'),
                Ticker.start_time,
                Ticker.response_time,
                Ticker.last_price,
                Ticker.last_trade,
                Ticker.best_ask,
                Ticker.best_bid,
                Ticker.daily_volume
            ],
            from_obj=(
                Ticker.__table__.join(ExchangeCurrencyPair, Ticker.exchange_pair_id == ExchangeCurrencyPair.id)
                    .join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id)
                    .join(first, ExchangeCurrencyPair.first_id == first.id)
                    .join(second, ExchangeCurrencyPair.second_id == second.id)
            )
        ),
        metadata=Base.metadata
    )

# Query for getting ecps with id
# select ecp.id, ecp.exchange_id, e.name, ecp.first_id, c1.name as first, ecp.second_id, c2.name as second from exchange e join exchanges_currency_pairs ecp on e.id=ecp.exchange_id join currencies c1 on ecp.first_id=c1.id join currencies c2 on ecp.second_id=c2.id;

# query for ecp-view
# select ecp.id, e.name as exchange_name, c1.name as first_name, c2.name as second_name from exchange e join exchanges_currency_pairs ecp on e.id=ecp.exchange_id join currencies c1 on ecp.first_id=c1.id join currencies c2 on ecp.second_id=c2.id;

# create ecp view
# create view exchange_currency_pairs as select ecp.id, e.name as exchange_name, c1.name as first_name, c2.name as second_name from exchange e join exchanges_currency_pairs ecp on e.id=ecp.exchange_id join currencies c1 on ecp.first_id=c1.id join currencies c2 on ecp.second_id=c2.id;

# ticker view query
# select ecp.exchange_name, ecp.first_name, ecp.second_name, t.start_time, t.response_time, t.last_price, t.best_ask, t.best_bid, t.daily_volume from tickers t inner join exchange_currency_pairs ecp on t.exchange_pair_id=ecp.id;

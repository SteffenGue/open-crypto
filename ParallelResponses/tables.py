from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
Base = declarative_base()  # pylint: disable=invalid-name
metadata = Base.metadata

class BaseMixin(object):

    """
    Classmethods for the database classes. 'update_exceptions()' adds +1 to the exception counter in Exchanges()
    whenever an exchange throws an exception. If more than x exceptions in a row occured, is_activ() sets the
    exchange 'inactive'. The exchange can only be set 'active' again manually.
    """

    @classmethod
    def is_active(cls, session):
        """
        Method to check if the number of exceptions raised for one exchange exceeds 3 in a row.
        If so, set exchange inactive.
        :param session: orm.session
            Actual session from the SessionFactory.
        :return: None
        """

        exchanges = session.query(Exchange).all()
        for exchange in exchanges:
            if exchange.exceptions > 3:
                exchange.active = False
                try:
                    session.commit()
                    print('{} was set inactive.'.format(exchange.name))
                except Exception as e:
                    print(e, e.__cause__)
                    session.rollback()
                    print('Exception raised setting {} inactive.'.format(exchange.name))
                    pass


    @classmethod
    def update_exceptions(cls, session, exceptions: dict):
        """
        Method to update the exception_counter. If An exception occured add 1 to the counter,
            else set back to zero.
        :param session: orm-session
            Actual session from the SessionFactory.
        :param exceptions: dict
            Dictionary with key (Exchange) value (boolean) pair.
        :return: None
        """
        exceptions = exceptions
        exchanges = list(session.query(Exchange).all())

        for exchange in exchanges:
            if exchange.name in exceptions:
                exchange.exceptions += 1
                exchange.total_exceptions += 1
                print('{}: Exception Counter +1'.format(exchange.name))
            else:
                exchange.exceptions = 0


        try:
            session.commit()
            cls.is_active(session)
        except Exception as e:
            print(e, e.__cause__)
            session.rollback()
            pass



class Exchange(BaseMixin, Base):
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
    active = Column(Boolean, default=True)
    exceptions = Column(Integer, unique=False, nullable=True, default=0)
    total_exceptions = Column(Integer, unique=False, nullable=True, default=0)

    def __repr__(self):
        return "#{}: {}, Active: {}".format(self.id, self.name, self.active)




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

    def __repr__(self):
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

    exchange_pair_id = Column(Integer, ForeignKey('exchanges_currency_pairs.id'), primary_key=True)
    exchange_pair = relationship('ExchangeCurrencyPairs', backref="tickers")

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



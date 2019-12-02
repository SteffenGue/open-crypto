from sqlalchemy import *
# SQLAlchemy declarative base class for tables and ORM classes
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()  # pylint: disable=invalid-name
metadata = Base.metadata


class Currency(Base):
    __tablename__ = 'currencies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True, nullable=False)
    symbol = Column(String(10), nullable=False)


class CurrencyPair(Base):
    __tablename__ = 'currency_pairs'

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    first_id = Column(Integer, ForeignKey(Currency.id), nullable=False)
    second_id = Column(Integer, ForeignKey(Currency.id), nullable=False)


class Exchange(Base):
    __tablename__ = 'exchanges'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False, unique=True)


# TODO Macht überhaupt Sinn?
class ExchangeCurrencyPair(Base):
    __tablename__ = 'exchange_currency_pairs'

    exchange_id = Column(Integer, ForeignKey(Exchange.id), primary_key=True, nullable=False)
    currency_pair_id = Column(Integer, ForeignKey(CurrencyPair.id), primary_key=True, nullable=False)


class Ticker(Base):
    __tablename__ = "tickers"

    exchange = Column(String, ForeignKey(Exchange.name), primary_key=True)
    first_currency = Column(String,
                            primary_key=True)
    second_currency = Column(String,
                             primary_key=True)
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

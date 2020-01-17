


from sqlalchemy import create_engine, Column, ForeignKey, Integer, String, CheckConstraint, column, Table, table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, relationships, backref
import psycopg2
import testing.postgresql
from sqlalchemy import exc


Base = declarative_base()



class Exchange(Base):
    __tablename__ = "exchanges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)

    currency = relationship("Currency",
                            secondary="exchange_pairs")

class Currency(Base):
    __tablename__ = 'currencies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)
    # exchange_id = Column(Integer, ForeignKey("exchanges.id"))
    exchange = relationship("Exchange",
                            secondary="exchange_pairs")


class ExchangePairs(Base):
    __tablename__ = 'exchange_pairs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'))
    first_id = Column(Integer, ForeignKey('currencies.id'), unique=True)
    second_id = Column(Integer, ForeignKey('currencies.id'), unique=True)

   #  __table_args__ = (CheckConstraint(first_id != second_id),)

    exchange = relationship("Exchange", backref=backref("currency_pairs", cascade="all, delete-orphan"))
    first = relationship("Currency", backref=backref("currency_pairs", cascade="all, delete-orphan"))
    #second = relationship("Currency", backref=backref("currency_pairs", cascade="all, delete-orphan"), foreign_keys="ExchangePairs.second_id")
   # second = relationship("Currency", foreign_keys="ExchangeCurrencyPairs.second_id")



engine = create_engine('sqlite:///:memory:', echo=True)
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)
session = Session()


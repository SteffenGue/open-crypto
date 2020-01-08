

"""
Dieses Skript demonstriert alternative Datenbankstrukturen. Die Datenbank kann entweder "in memory" oder als ".db-file" abgelegt werden. 
Sinn und Ziel ist der Aufbau und Test verschiedenster Datenbankstrukturen und -relationen für eine möglichst tief integrierte Datenbank.

Zu Erstellung der Datenbank in Memory/db-File: 
        - create_engine('sqlite:///:memory:', echo = True)
        - create_engine('sqlite:///relationen.db', echo=True)


Datenbankaufbau:

    Tables:
        - Parent: Exchange, Currencies
        - Child: ExchangeCurrencies
        - "Grandchild": ExchangePairs


    Relationen:
        - Many-to-Many Relationen werden aufgebrochen in zwei One-to-Many Relationen mit einem Association table
          dazwischengeschaltet.
        - Exchange und Currency (jeweils "one") können nur einmal hinterlegt sein (Unique!), allerdings kann eine
          Exchange mehrere Currencies und eine Currency mehrere Exchanges haben (ExchangeCurrencies "Many"). Das
          ergibt in Summe eine geordnete Many-to-Many Relation.
        - Das gleiche Spiel mit ExchangeCurrencies und ExchangePairs. Eine ExchangeCurrency kann nur einmal hinter-
          legt sein, allerdings kan eine in mehreren Paaren vertreten sein. Daher ExchangeCurrencies (One) und
          ExchangePairs (Many).


    Hinweis: Zunächst etwas verwirrend sind die Relationships. Die Argumente sind folgende:
                1. Das Table (Klasse)
                2. Das association table 
                3. Das Column in dem jeweiligen Table.
            
            Der Befehl "backref" macht im Endeffekt das gleiche, spart sich aber eine Hälfte der Verbindung.
            Heißt, man müsste mit dem backref-Befehl nur eine von zwei "relationships" ausschreiben. Das ist 
            im nachhinein allerdings schwerer nachvollziehbar, weshalb ich "back_population" bevorzuge.

"""

from sqlalchemy import create_engine, Column, ForeignKey, Integer, String, CheckConstraint, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()


class Exchange(Base):
    __tablename__ = "exchanges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)

    exchange_currencies = relationship('ExchangeCurrencies',
                                       back_populates="exchange")


class Currency(Base):
    __tablename__ = 'currencies'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)

    currency_exchanges = relationship('ExchangeCurrencies',
                                      back_populates='currency')


class ExchangeCurrencies(Base):
    __tablename__ = 'exchanges_currencies'
    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'))
    currency_id = Column(Integer, ForeignKey('currencies.id'))

    exchange = relationship('Exchange', back_populates="exchange_currencies")
    currency = relationship('Currency', back_populates="currency_exchanges")


class ExchangePairs(Base):
    __tablename__ = 'exchanges_pairs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, ForeignKey('exchanges_currencies.exchange_id'))
    first_id = Column(Integer, ForeignKey('exchanges_currencies.currency_id'))
    second_id = Column(Integer, ForeignKey('exchanges_currencies.currency_id'))

    __table_args__ = (CheckConstraint(first_id != second_id),)

    first = relationship("ExchangeCurrencies", foreign_keys="ExchangePairs.first_id")
    second = relationship("ExchangeCurrencies", foreign_keys="ExchangePairs.second_id")


engine = create_engine('sqlite:///:memory:', echo=True)
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)
session = Session()

test = ExchangePairs(first=ExchangeCurrencies(exchange=Exchange(name="a"),
                                              currency=Currency(name="b")),
                     second=ExchangeCurrencies(exchange=Exchange(name="c"),
                                               currency=Currency(name="b")))


session.add(test)
session.commit()

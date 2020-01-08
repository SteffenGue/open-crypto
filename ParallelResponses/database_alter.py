

"""
Dieses Skript demonstriert alternative Datenbankstrukturen. Die Datenbank kann entweder "in memory" oder als ".db-file" abgelegt werden.
Sinn und Ziel ist der Aufbau und Test verschiedenster Datenbankstrukturen und -relationen für eine möglichst tief integrierte Datenbank.

Zu Erstellung der Datenbank in Memory/db-File:
        - create_engine('sqlite:///:memory:', echo = True)
        - create_engine('sqlite:///relationen.db', echo=True)


Datenbankaufbau:

    Tables:
        - Parent: Exchange, Currencies
        - Child: ExchangeCurrency_Pairs

    Relationen:
        - Many-to-Many Relationen werden aufgebrochen in zwei One-to-Many Relationen mit einem Association table
          dazwischengeschaltet.
        - Exchange und Currency (jeweils "one") können nur einmal hinterlegt sein (Unique!), allerdings kann eine
          Exchange mehrere Currencies und eine Currency mehrere Exchanges haben (ExchangeCurrencies "Many"). Das
          ergibt in Summe eine geordnete Many-to-Many Relation.
    Vorteil:
        - einfache Struktur, übersichtliche Relationen (One-to-Many, bidirektional)

    Nachteile:
        - Tables Currency und Exchange sind nicht verknüpft. Eine Zuordnung der Currencies zu einer Exchange findet
            somit nur indirekt über das Table ExchangeCurrencies statt. Das Verknüpfen der beiden Tables per
            Relationship scheitert bisher an dem doppelten ForeignKey auf die "currencies.id".

"""

from sqlalchemy import create_engine, Column, ForeignKey, Integer, String, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()


class Exchange(Base):
    __tablename__ = "exchanges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)


class Currency(Base):
    __tablename__ = 'currencies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)


class ExchangeCurrencyPairs(Base):
    __tablename__ = 'exchanges_currency_pairs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'))
    first_id = Column(Integer, ForeignKey('currencies.id'), unique=True)
    second_id = Column(Integer, ForeignKey('currencies.id'), unique=True)

    __table_args__ = (CheckConstraint(first_id != second_id),)

    exchange = relationship("Exchange", backref="exchanges_currency_pairs")
    first = relationship("Currency", foreign_keys="ExchangeCurrencyPairs.first_id")
    second = relationship("Currency", foreign_keys="ExchangeCurrencyPairs.second_id")



engine = create_engine('sqlite:///:memory:', echo=True)
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)
session = Session()

test = ExchangeCurrencyPairs(exchange=Exchange(name="a"),
                             first=Currency(name="b"),
                             second=Currency(name="c"))

session.add(test)
session.commit()
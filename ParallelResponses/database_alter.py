
from sqlalchemy import create_engine, Column, ForeignKey, Integer, String, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

"""
Dieses Skript demonstriert alternative Datenbankstrukturen. Die Datenbank kann entweder "in memory" oder als ".db-file" abgelegt werden. 
Sinn und Ziel ist der Aufbau und Test verschiedenster Datenbankstrukturen und -relationen für eine möglichst tief integrierte Datenbank.

Zu Erstellung der Datenbank in Memory/db-File: 
        - create_engine('sqlite:///:memory:', echo = True)
        - create_engine('sqlite:///relationen.db', echo=True)
        
        
Datenbankaufbau:
    
    Tables: 
        - Parent: Exchange, Currencies
        - Child: ExchangeCurrency 
    
    Relationen:
        - ForeignKeys auf: Exchange.id, Currency.id (2x) für first.id, second.id   
        
    Vorteil: 
        - einfache Struktur, übersichtliche Relationen (One-to-Many, bidirektional)
    
    Nachteile: 
        - Tables Currency und Exchange sind nicht verknüpft. Eine Zuordnung der Currencies zu einer Exchange findet 
            somit nur indirekt über das Table ExchangeCurrencies statt. Das Verknüpfen der beiden Tables per 
            Relationship scheitert bisher an dem doppelten ForeignKey auf die "currencies.id".
        
"""

Base = declarative_base()


class ExchangeCurrency(Base):

    __tablename__ = 'exchanges_currencies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'))
    first_id = Column(Integer, ForeignKey('currencies.id'), unique=True)
    second_id = Column(Integer, ForeignKey('currencies.id'), unique= True)

    __table_args__ = (CheckConstraint(first_id != second_id),)

    exchange = relationship("Exchange", backref="exchanges_currencies")
    first = relationship("Currency", foreign_keys= "ExchangeCurrency.first_id")
    second = relationship("Currency", foreign_keys= "ExchangeCurrency.second_id")



class Exchange(Base):

    __tablename__ = "exchanges"

    id = Column(Integer, primary_key= True, autoincrement=True)
    name = Column(String, unique= True)



class Currency(Base):

    __tablename__ = 'currencies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)



engine = create_engine('sqlite:///:memory:', echo = True)
Base.metadata.create_all(bind= engine)
Session = sessionmaker(bind=engine)
session = Session()


exchange = Exchange(name ="bibox")
exchange2 = Exchange(name="aidos")

currency1 = Currency(name = "Bitcoin")
currency2 = Currency(name = "Litecoin")

ExCu1 = ExchangeCurrency(exchange_id=1, first_id=1, second_id=2)
ExCu2 = ExchangeCurrency(exchange_id=2, first_id=2, second_id =1)

liste = [exchange, exchange2, currency1, currency2, ExCu1, ExCu2]

session.add_all(liste)
session.commit()


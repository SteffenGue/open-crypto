"""
Dieses Skript demonstriert alternative Datenbankstrukturen. Die Datenbank kann entweder "in memory" oder als ".db-file" abgelegt werden. 
Sinn und Ziel ist der Aufbau und Test verschiedenster Datenbankstrukturen und -relationen für eine möglichst tief integrierte Datenbank.

Zu Erstellung der Datenbank in Memory/db-File: 
        - create_engine('sqlite:///:memory:', echo = True)
        - create_engine('sqlite:///relationen.db', echo=True)


Datenbankaufbau:

    Relationen:
        - Bidirektionale Many-to-Many Relationship zwischen Exchange, Currency und dem "association_table" ExchangeCurrency. 

    Problem: 
        - Der doppelte ForeignKey vom "association_table" auf "currencies.id" funktioniert nicht. 
            SqlAlchemy schmeißt hier einen "AmbiguousForeignKeysError".

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


class ExchangeCurrency(Base):
    __tablename__ = 'exchanges_currencies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'))
    first_id = Column(Integer, ForeignKey('currencies.id'))
    second_id = Column(Integer, ForeignKey('currencies.id'))

    __table_args__ = (CheckConstraint(first_id != second_id),)


class Exchange(Base):
    __tablename__ = "exchanges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)

    currencies = relationship("Currency",
                              secondary="exchanges_currencies",
                              back_populates = "exchanges")

class Currency(Base):
    __tablename__ = 'currencies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)

    exchanges = relationship("Exchange",
                             secondary="exchanges_currencies",
                             back_populates= "currencies")

engine = create_engine('sqlite:///:memory:', echo=True)
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)
session = Session()

exchange = Exchange(name="bibox")
exchange2 = Exchange(name="aidos")

currency1 = Currency(name="Bitcoin")
currency2 = Currency(name="Litecoin")

ExCu1 = ExchangeCurrency(exchange_id=1, first_id=1, second_id=2)
ExCu2 = ExchangeCurrency(exchange_id=2, first_id=2, second_id=1)

liste = [exchange, exchange2, currency1, currency2, ExCu1, ExCu2]


session.add_all(liste)
session.commit()


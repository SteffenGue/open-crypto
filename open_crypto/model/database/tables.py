#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module provides the database defining classes as well as specific views on all unreadable database tables.

Classes:
 - Exchange: Defines the exchanges table.
 - Currency: Defines the currencies table.
 - ExchangesCurrencyPair: Defines the exchanges_currency_pairs table.
 - Ticker: Defines the tickers tables.
 - Trade: Defines the trades table.
 - HistoricRate: Defines the historic_rates table.
 - OrderBook: Defines the order_books table.

 - ExchangeCurrencyPairView: Provides a view on the exchanges_currency_pairs table.
 - TickerView: Provides a view on the tickers table.
 - TradeView: Provides a view on the trades table.
 - HistoricRateView: Provides a view on the historic_rates table.
 - OrderBookView: Provides a view on the order_books table.
"""

from typing import Union, Type

from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, CheckConstraint, Float, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, validates, aliased
from sqlalchemy_utils import create_view

from model.database.type_decorators import UnixTimestampMs

Base = declarative_base()  # pylint: disable=invalid-name
metadata = Base.metadata


class Exchange(Base):
    """
    Database ORM-Class storing the exchange table. ALl exchange used to perform requests
    are listed in this table.

    id: int
        Auto incremented unique identifier.
    name: str
        The explicit name of the exchange defined in the .yaml-file.
    """
    __tablename__ = "exchanges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name: Column = Column(String(50), nullable=False, unique=True)
    active = Column(Boolean, default=True)
    is_exchange = Column(Boolean, default=True)
    exceptions = Column(Integer, unique=False, nullable=True, default=0)
    total_exceptions = Column(Integer, unique=False, nullable=True, default=0)

    def __repr__(self) -> str:
        return f"#{self.id}: {self.name}, Active: {self.active}"

    @validates("name")
    def convert_upper(self, _: str, value: str) -> str:
        """
        Converts strings into upper cases.
        """
        return value.upper()


class Currency(Base):
    """
    Database ORM-Class storing all currencies.

    id: int
        Auto incremented unique identifier.
    name: str
        Name of the currency written out.
    symbol: str
        Abbreviation of the currency
    """
    __tablename__ = "currencies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True, nullable=False)
    from_exchange = Column(Boolean, default=True)

    def __repr__(self) -> str:
        return f"#{self.id}: {self.name}"

    @validates("name")
    def convert_upper(self, _: str, value: str) -> str:
        """
        Converts strings into upper cases.
        """
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
    __tablename__ = "exchanges_currency_pairs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"))
    first_id = Column(Integer, ForeignKey("currencies.id"))
    second_id = Column(Integer, ForeignKey("currencies.id"))

    exchange = relationship("Exchange", backref="exchanges_currency_pairs", lazy="joined")
    first = relationship("Currency", foreign_keys="ExchangeCurrencyPair.first_id", lazy="joined")
    second = relationship("Currency", foreign_keys="ExchangeCurrencyPair.second_id", lazy="joined")

    __table_args__ = (CheckConstraint(first_id != second_id),)

    def __repr__(self) -> str:
        return f"#{self.id}: {self.exchange.name}({self.exchange_id}), " \
               f"{self.first.name}({self.first_id})-{self.second.name}({self.second_id})"

    def __str__(self) -> str:
        return self.__repr__()


class Ticker(Base):
    """
    Database ORM-Class storing the ticker data.

    exchange_pair_id: int
        Unique Exchange_Currency_Pair identifier. The exchange_pair_id is used by the database_handler to check
         for existing exchange_currency_pairs. If not existing, the currency and/or exchange is created.

    exchange_pair: relationship
        The corresponding relationship table with ExchangeCurrencyPair

    start_time: UnixTimestamp
        Timestamp of the execution of an exchange request (UTC). Timestamps are unique for each exchange.

    response_time: UnixTimestamp
        Timestamp of the response. Timestamps are created by the OS, the delivered ones from the exchange are not used.
        Timestamps are equal for each exchange for one run (resulting in an error of approx. 5 seconds average)
         to ease data usage later.
        Timestamps are rounded to seconds (UTC)

    last_price: float
        Latest price of the currency_pair given from the exchange.
    best_ask: float
        Best ask price of an exchange for a currency_pair.
    best_bid: float
        Best bid price of an exchange for a currency_pair.
    daily_volume: float
        The traded volume of an currency_pair on an exchange. Definition can differ for each exchange!

    """
    __tablename__ = "tickers"

    exchange_pair_id = Column(Integer, ForeignKey("exchanges_currency_pairs.id"), primary_key=True)
    exchange_pair = relationship("ExchangeCurrencyPair", backref="tickers")

    start_time = Column(UnixTimestampMs)
    time = Column(UnixTimestampMs, primary_key=True)
    last_price = Column(Float)
    best_ask = Column(Float)
    best_bid = Column(Float)

    def __repr__(self) -> str:
        return f"#{self.exchange_pair_id}, {self.exchange_pair.exchange.name}: " \
               f"{self.exchange_pair.first.name}-{self.exchange_pair.second.name}, ${self.last_price} at {self.time}"


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
    __tablename__ = "historic_rates"

    exchange_pair_id = Column(Integer, ForeignKey("exchanges_currency_pairs.id"), primary_key=True)
    exchange_pair = relationship("ExchangeCurrencyPair", backref="historic_rates")
    time = Column(UnixTimestampMs, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    # base_volume = Column(Float)  # ToDo: check if base_volume is correct.
    market_cap = Column(Float)

    def __repr__(self) -> str:
        return f"ID {self.exchange_pair_id}, {self.exchange_pair.exchange.name}: " \
               f"{self.exchange_pair.first.name}-{self.exchange_pair.second.name}, close {self.close} at {self.time}"


class Trade(Base):
    """
    Table for the method trades. Tables contains the exchange_currency_pair_id, gathered from the
    foreign_keys.
    Primary_keys are Exchange_Pair_id and the timestamp.

    Table contains the last trades, trade amount, trade direction (buy/sell) and timestamp.

    __repr__(self) describes the representation if queried.

    """
    __tablename__ = "trades"

    exchange_pair_id = Column(Integer, ForeignKey("exchanges_currency_pairs.id"), primary_key=True)
    exchange_pair = relationship("ExchangeCurrencyPair", backref="trades")
    id = Column(Integer, primary_key=True)
    time = Column(UnixTimestampMs, primary_key=True)

    amount = Column(Float, primary_key=True)
    best_bid = Column(Float)
    best_ask = Column(Float)
    price = Column(Float)
    _direction = Column("direction", String) # This is changes from Integer to String as the Hybrid Property method
                                             # does currently not work with insert().on_conflict_do_nothing().
                                             # It Does however work with the standard session.add(Some_Instance)

    @hybrid_property
    def direction(self) -> Union[str, int]:
        """
        Returns the direction.
        #ToDo: Make Hybrid Property work again with insert.on_conflict_do_nothing() method.
        """
        return self._direction

    @direction.setter
    def direction(self, direction: Union[str, int]) -> None:
        """
        Converts the string representation of the trade direction, i.e. 'sell' or 'buy', into an integer.

        @param: String representation of the direction.
        """
        if isinstance(direction, str):
            if direction.lower() == "sell":
                self._direction = 0
            elif direction.lower() == "buy":
                self._direction = 1
            else:
                self._direction = direction

    def __repr__(self) -> str:
        return f"Last Transaction: {self.exchange_pair.exchange.name}, {self.exchange_pair.first.name}-" \
               f"{self.exchange_pair.second.name}: {self.amount} for {self.price} at {self.time}"

    @validates("direction")
    def convert_upper(self, _: str, value: str) -> str:
        """
        Converts strings into upper cases.
        """
        return value.upper()


class OrderBook(Base):
    """
    Table for the method order-books. Tables contains the exchange_currency_pair_id, gathered from the
    foreign_keys.

    Primary_keys are Exchange_Pair_id, id, and position.

    Table next to the bids and asks (both with Price and Amount) the position which indicates the position in
    the order book at given time. I.e position 0 contains the highest Bid and the lowest Ask. The ID is gathered
    directly from the exchange and is used to identify to identify changes in the order-book.
    """
    __tablename__ = "order_books"

    exchange_pair_id = Column(Integer, ForeignKey("exchanges_currency_pairs.id"), primary_key=True)
    exchange_pair = relationship("ExchangeCurrencyPair", backref="OrderBook")

    id = Column(Integer, primary_key=True)
    position = Column(Integer, primary_key=True)

    time = Column(UnixTimestampMs)
    bids_amount = Column(Float)
    bids_price = Column(Float)
    asks_price = Column(Float)
    asks_amount = Column(Float)


class ExchangeCurrencyPairView(Base):
    """
    View vor ExchangeCurrencyPairs.
    """
    first = aliased(Currency)
    second = aliased(Currency)

    __table__ = create_view(
        name="exchanges_currency_pairs_view",
        selectable=select(
            [
                ExchangeCurrencyPair.id,
                Exchange.name.label("exchange_name"),
                first.name.label("first_name"),
                second.name.label("second_name"),
            ],
            from_obj=(
                ExchangeCurrencyPair.__table__.join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id)
                                              .join(first, ExchangeCurrencyPair.first_id == first.id)
                                              .join(second, ExchangeCurrencyPair.second_id == second.id)
            )
        ),
        metadata=Base.metadata
    )


class TickerView(Base):
    """
    View for Tickers.
    Instead of only showing the ID of the ExchangeCurrencyPair the View displays
    the exchange name and the name of the first and second currency.
    """
    first = aliased(Currency)
    second = aliased(Currency)
    __table__ = create_view(
        name="tickers_view",
        selectable=select(
            [
                Exchange.name.label("exchange"),
                first.name.label("first_currency"),
                second.name.label("second_currency"),
                Ticker.start_time,
                Ticker.time,
                Ticker.last_price,
                Ticker.best_ask,
                Ticker.best_bid,
                # Ticker.daily_volume
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


class TradeView(Base):
    """
    View for Trades.
    Instead of only showing the ID of the ExchangeCurrencyPair the View displays
    the exchange name and the name of the first and second currency.
    """
    first = aliased(Currency)
    second = aliased(Currency)

    __table__ = create_view(
        name="trades_view",
        selectable=select(
            [
                Exchange.name.label("exchange"),
                first.name.label("first_currency"),
                second.name.label("second_currency"),
                Trade.id,
                Trade.time,
                Trade.amount,
                Trade.best_ask,
                Trade.best_bid,
                Trade.price,
                Trade.direction,
            ],
            from_obj=(
                Trade.__table__.join(ExchangeCurrencyPair, Trade.exchange_pair_id == ExchangeCurrencyPair.id)
                               .join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id)
                               .join(first, ExchangeCurrencyPair.first_id == first.id)
                               .join(second, ExchangeCurrencyPair.second_id == second.id)
            )
        ),
        metadata=Base.metadata
    )


class OrderBookView(Base):
    """
    View for Order-Books.
    Instead of only showing the ID of the ExchangeCurrencyPair the View displays
    the exchange name and the name of the first and second currency.
    """
    first = aliased(Currency)
    second = aliased(Currency)
    __table__ = create_view(
        name="order_books_view",
        selectable=select(
            [
                Exchange.name.label("exchange"),
                first.name.label("first_currency"),
                second.name.label("second_currency"),
                OrderBook.id,
                OrderBook.position,
                OrderBook.time,
                OrderBook.bids_amount,
                OrderBook.bids_price,
                OrderBook.asks_price,
                OrderBook.asks_amount
            ],
            from_obj=(
                OrderBook.__table__.join(ExchangeCurrencyPair, OrderBook.exchange_pair_id == ExchangeCurrencyPair.id)
                                   .join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id)
                                   .join(first, ExchangeCurrencyPair.first_id == first.id)
                                   .join(second, ExchangeCurrencyPair.second_id == second.id)
            )
        ),
        metadata=Base.metadata
    )


class HistoricRateView(Base):
    """
    View for Historic-Rates.
    Instead of only showing the ID of the ExchangeCurrencyPair the View displays
    the exchange name and the name of the first and second currency.
    """
    first = aliased(Currency)
    second = aliased(Currency)
    __table__ = create_view(
        name="historic_rates_view",
        selectable=select(
            [
                Exchange.name.label("exchange"),
                first.name.label("first_currency"),
                second.name.label("second_currency"),
                HistoricRate.time,
                HistoricRate.open,
                HistoricRate.high,
                HistoricRate.low,
                HistoricRate.close,
                HistoricRate.volume,
                # HistoricRate.base_volume,
                HistoricRate.market_cap,
            ],
            from_obj=(
                HistoricRate.__table__.join(ExchangeCurrencyPair,
                                            HistoricRate.exchange_pair_id == ExchangeCurrencyPair.id)
                                      .join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id)
                                      .join(first, ExchangeCurrencyPair.first_id == first.id)
                                      .join(second, ExchangeCurrencyPair.second_id == second.id)
            )
        ),
        metadata=Base.metadata
    )


DatabaseTable = Union[Exchange, Currency, ExchangeCurrencyPair, Ticker, HistoricRate, Trade, OrderBook]

DatabaseTableType = Union[Type[Exchange], Type[Currency], Type[ExchangeCurrencyPair], Type[Ticker], Type[HistoricRate],
                          Type[Trade], Type[OrderBook]]

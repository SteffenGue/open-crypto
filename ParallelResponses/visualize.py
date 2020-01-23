import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

import sqlalchemy
from sqlalchemy_utils import database_exists
from sqlalchemy.orm import sessionmaker, session, create_session
from sqlalchemy import create_engine, and_
from tables import Currency, Exchange, ExchangeCurrencyPairs, Ticker

engine = create_engine('postgresql+psycopg2://postgres:Thw_kiel@localhost:1234/hochfrequenz_test')

Session = sessionmaker(bind=engine)

session = Session()


(bitcoin,), = session.query(Currency.id).filter(Currency.name == "LTC").all()
(tether,),  = session.query(Currency.id).filter(Currency.name == "USDT").all()
(dollar,), = session.query(Currency.id).filter(Currency.name == "USD").all()


btc_usd = session.query(ExchangeCurrencyPairs.exchange_id).filter(ExchangeCurrencyPairs.first_id == bitcoin, ExchangeCurrencyPairs.second_id == dollar).all()
btc_usdt = session.query(ExchangeCurrencyPairs.exchange_id).filter(ExchangeCurrencyPairs.first_id == bitcoin, ExchangeCurrencyPairs.second_id == tether).all()
exchanges = [value for value, in set(btc_usd) & set(btc_usdt)]
exchanges.sort()

tickers=list()
df = pd.DataFrame()


for exchange in exchanges:
    (btc_usd,), = session.query(ExchangeCurrencyPairs.id).filter(ExchangeCurrencyPairs.exchange_id == exchange,
                                                             ExchangeCurrencyPairs.first_id == bitcoin,
                                                             ExchangeCurrencyPairs.second_id == dollar).all()

    (btc_usdt,), = session.query(ExchangeCurrencyPairs.id).filter(ExchangeCurrencyPairs.exchange_id == exchange,
                                                              ExchangeCurrencyPairs.first_id == bitcoin,
                                                              ExchangeCurrencyPairs.second_id == tether).all()

    ticker = session.query(Ticker.response_time, Ticker.last_price).filter(Ticker.exchange_pair_id==btc_usd).all()
    ticker2 = session.query(Ticker.response_time, Ticker.last_price).filter(Ticker.exchange_pair_id==btc_usdt).all()

    df = pd.DataFrame(ticker)
    df.set_index('response_time', inplace=True)
    df.index = pd.to_datetime(df.index)

    df2 = pd.DataFrame(ticker2)
    df2.set_index('response_time', inplace=True)
    df.index = pd.to_datetime(df.index)


    # plt.plot(df/df2, label=exchange)
    # plt.legend()
    # plt.title("BTC/USD vs BTC/USDT Ratio")
    # plt.show()


    if exchange == min(exchanges):
        ax = (df.iloc[:,0]/df2.iloc[:,0]).plot(label=str(exchange))
    else:
        (df.iloc[:,0]/df2.iloc[:,0]).plot(ax=ax, label=exchange)
    ax.legend()
plt.show()


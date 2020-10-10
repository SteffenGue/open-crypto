from pandas import DataFrame
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.orm import sessionmaker, Query, aliased
import matplotlib.pyplot as plt

from model.database.tables import ExchangeCurrencyPair, Exchange, Currency, Ticker, TickersView

# engine = create_engine('sqlite:///hochfrequenz.db')
engine = create_engine('postgresql+psycopg2://bjarne:@localhost:5432/hochfrequenz')
session = sessionmaker(bind=engine)


def get_ecps_for_first(currency: str) -> [int]:
    first_id = session().query(Currency.id).filter(currency.upper() == Currency.name).scalar()
    print(first_id)
    return [r[0] for r in
            session().query(ExchangeCurrencyPair.id).filter(ExchangeCurrencyPair.first_id == first_id).all()]


def get_dfs_for_pair(first_name: str, second_name: str):
    first = aliased(Currency)
    second = aliased(Currency)

    sess = session()
    first_id = sess.query(Currency.id).filter(Currency.name == first_name.upper()).scalar()
    second_id = sess.query(Currency.id).filter(Currency.name == second_name.upper()).scalar()
    exchange_ids = [r[0] for r in sess.query(Exchange.id).all()]

    result: DataFrame = DataFrame()
    first_frame = True
    for exchange_id in exchange_ids:
        exchange_pair_id = sess.query(ExchangeCurrencyPair.id). \
            filter(ExchangeCurrencyPair.exchange_id == exchange_id). \
            filter(ExchangeCurrencyPair.first_id == first_id). \
            filter(ExchangeCurrencyPair.second_id == second_id). \
            scalar()

        if exchange_pair_id:
            data: Query = sess.query(Exchange.name.label('exchange'),
                                     first.name.label('first_currency'),
                                     second.name.label('second_currency'),
                                     Ticker.start_time,
                                     Ticker.last_price). \
                join(ExchangeCurrencyPair, Ticker.exchange_pair_id == ExchangeCurrencyPair.id). \
                join(Exchange, ExchangeCurrencyPair.exchange_id == Exchange.id). \
                join(first, ExchangeCurrencyPair.first_id == first.id). \
                join(second, ExchangeCurrencyPair.second_id == second.id). \
                filter(ExchangeCurrencyPair.id == exchange_pair_id). \
                order_by(Ticker.start_time)

            df = pd.read_sql_query(data.statement, con=sess.bind)
            print(df)
            if any(df['last_price'].values):
                if first_frame:
                    first_frame = False
                    result = DataFrame(df['start_time'])

                result.insert(1, df['exchange'][0], df['last_price'], True)
    return result


if __name__ == '__main__':
    df = pd.read_sql_table('tickers_view', con=engine)
    dataframe: [Ticker] = get_dfs_for_pair('eth', 'usdt')

    for col in dataframe.columns[1:6]:
        plt.plot('start_time', col, data=dataframe, marker='', )

    plt.legend()
    plt.show()
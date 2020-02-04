import asyncio
import os
import psycopg2
import time
from datetime import datetime, timedelta
from db_handler import DatabaseHandler
from exchanges.exchange import Exchange
from tables import metadata
from utilities import read_config, yaml_loader, get_exchange_names
from dictionary import ExceptionDict


async def main():
    """
    The main() function to run the program. Loads the database, including the database_handler.
    The exchange_names are extracted with a helper method in utilities based on existing yaml-files.
    In an asynchronous manner it is iterated over the exchanges and and the responses are awaited and collected
        by await asyncio.gather(..)
    As soon as all responses from the exchanges are returned, the values get extracted, formatted into tuples
        by the exchange.get_ticker(..) method and persisted by the into the database by the database_handler.
    """

    db_params = read_config('database')
    database_handler = DatabaseHandler(metadata, **db_params)
    #exception_dict = ExceptionDict()

    # run program with single exchange for debugging/testing purposes
    # exchange_names = ['vindax']


    exceptions = {'bitrue': 1}

    exchange_names = get_exchange_names()

    exchanges = {exchange_name: Exchange(yaml_loader(exchange_name)) for exchange_name in exchange_names}
    # start_time : datetime when request run is started
    # delta : given microseconds for the datetime
    start_time = datetime.utcnow()
    delta = start_time.microsecond
    # rounding the given datetime on seconds
    start_time = start_time - timedelta(microseconds=delta)
    if delta >= 500000:
        start_time = start_time + timedelta(seconds=1)

    responses = await asyncio.gather(*(exchanges[ex].request('ticker', start_time) for ex in exchanges))


    for response in responses:
        print('Response: {}'.format(response))
        if response:
            exchange = exchanges[response[0]]
            formatted_response = exchange.format_ticker(response)
            database_handler.persist_tickers(formatted_response)

    database_handler.check_exceptions(exceptions)

if __name__ == "__main__":
    try:
        while True:
            asyncio.run(main())
            print("5 Minuten Pause.")
            time.sleep(300)
    except Exception as e:
        print(e, e.__cause__)
        pass

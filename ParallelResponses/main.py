import asyncio
import psycopg2
import os
import time
from datetime import datetime, timedelta
from db_handler import DatabaseHandler
from exchanges.exchange import Exchange
from tables import metadata
from utilities import read_config, yaml_loader, get_exchange_names, REQUEST_PARAMS


async def main(exchange_names):
    """
    The main() function to run the program. Loads the database, including the database_handler.
    The exchange_names are extracted with a helper method in utilities based on existing yaml-files.
    In an asynchronous manner it is iterated over the exchanges and and the responses are awaited and collected
        by await asyncio.gather(..)
    As soon as all responses from the exchanges are returned, the values get extracted, formatted into tuples
        by the exchange.get_ticker(..) method and persisted by the into the database by the database_handler.
    :param exchange_names list
        The list of names of the exchanges which will be requested
    """

    exchanges = {exchange_name: Exchange(yaml_loader(exchange_name), database_handler.request_params)
                 for exchange_name in exchange_names}

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
        if response:
            print('Response: {}'.format(response))
            exchange = exchanges[response[0]]
            formatted_response = exchange.format_ticker(response)
            database_handler.persist_tickers(formatted_response)

    # variables of flag will be updated
    for exchange in exchanges:
        exchanges[exchange].update_exception_counter()
    # variables in database will be updated because of information purpose
    database_handler.update_active_flag(exchanges)
    # variables of exception counter will be updated
    for exchange in exchanges:
        exchanges[exchange].update_consecutive_exception()


if __name__ == "__main__":
    try:

        db_params = read_config('database')
        database_handler = DatabaseHandler(metadata, **db_params)
        while True:
            #todo: secondary list of exchanges ( passive exchanges )

            # run program with single exchange for debugging/testing purposes
            # exchanges_names = ['coinsbit']
            exchange_names = get_exchange_names(database_handler.get_active_exchanges)
            asyncio.run(main(exchange_names))

            # todo: to update the list of the exchanges which will be send requests

            print("5 Minuten Pause.")
            time.sleep(300)
    except Exception as e:
        print(e, e.__cause__)
        pass

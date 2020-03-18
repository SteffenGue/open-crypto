import asyncio
import time
from datetime import datetime, timedelta
from db_handler import DatabaseHandler
from exchanges.exchange import Exchange
from tables import metadata
from utilities import read_config, yaml_loader, get_exchange_names, REQUEST_PARAMS
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
    # run program with single exchange for debugging/testing purposes
    # exchange_names = ['coinsbit']

    exchange_names = get_exchange_names(database_handler.get_active_exchanges)
    exchanges = {exchange_name: Exchange(yaml_loader(exchange_name), database_handler.request_params)
                 for exchange_name in exchange_names}

    exchanges = {exchange_name: Exchange(yaml_loader(exchange_name)) for exchange_name in exchange_names}

    # start_time : datetime when request run is started
    # delta : given microseconds for the datetime
    start_time = datetime.utcnow()
    delta = start_time.microsecond
    # rounding the given datetime on seconds
    start_time = start_time - timedelta(microseconds=delta)
    if delta >= 500000:
        start_time = start_time + timedelta(seconds=1)

    # responses = await asyncio.gather(*(exchanges[ex].request('ticker', start_time) for ex in exchanges))
    for ex in exchanges:
        currency_pairs = database_handler.get_exchange_currency_pairs(ex)

    for response in responses:
        if response:
            print('Response: {}'.format(response))
            exchange = exchanges[response[0]]
            formatted_response = exchange.format_ticker(response)
            database_handler.persist_tickers(formatted_response)

    #exceptions : instance of the dictionary of exceptions for the request run
    #with method call to check and persist the flags with the given exceptions
    exceptions = ExceptionDict()
    database_handler.update_exceptions(exceptions.get_dict())
    exceptions.get_dict().clear()

if __name__ == "__main__":
    try:
        while True:
            asyncio.run(main())
            print("5 Minuten Pause.")
            time.sleep(300)
    except Exception as e:
        print(e, e.__cause__)
        pass

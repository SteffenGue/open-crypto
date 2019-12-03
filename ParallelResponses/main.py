import asyncio
import os
from bs4 import BeautifulSoup
import urllib.request
from db_handler import DatabaseHandler
from exchanges.exchange import Exchange
from tables import metadata
from utilities import read_config, yaml_loader


async def main():

    """
    The main() function to run the program. Loads the database, including the database_handler.
    The exchange_names are extracted from the specified directory, extracted from the filenames
        and further sorted from "A-Z".
    In an asynchronous manner it is iterated over the exchanges and and the responses are awaited and collected
        by await asyncio.gather(..)
    As soon as all responses from the exchanges are returned, the values get extracted, formatted into tuples
        by the exchange.get_ticker(..) method and persisted by the into the database by the database_handler.
    """

    db_params = read_config('database')
    database_handler = DatabaseHandler(metadata, **db_params)

    # run program with single exchange for debugging/testing purposes
    # exchange_names = ['vindax']

    # Extracting all file names from the directory
    exchanges_list = os.listdir('resources/running_exchanges')
    exchange_names = [x.split(".")[0] for x in exchanges_list if ".yaml" in x]
    exchange_names.sort()

    database_handler.persist_exchanges(exchange_names)

    exchanges = {exchange_name: Exchange(yaml_loader(exchange_name)) for exchange_name in exchange_names}

    responses = await asyncio.gather(*(exchanges[ex].request('ticker') for ex in exchanges))

    for response in responses:
        print('Response: {}'.format(response))
        if response:
            exchange = exchanges[response[0]]
            formatted_response = exchange.format_ticker(response)
            database_handler.persist_tickers(formatted_response)


if __name__ == "__main__":
    asyncio.run(main())



# <--------------------- Currency-Pair Methods (currently no use) --------------------->
"""
Method to scrape currencies from coinmarketcap.com. Method is currently not in use 
as the condition to persist response tuples if the currency exists in the database
is not longer activated. Moreover a new issue was created (03.12.2019) in GitLab to 
automatically update the currencies/currency_pairs/exchange_currency_pairs tables if new
currencies are discovered in the responses. 
"""

# def get_coins() -> list:
#     fp = urllib.request.urlopen("https://coinmarketcap.com/all/views/all/")
#     soup = BeautifulSoup(fp, 'html.parser')
#     fp.close()
#
#     coin_rows = soup.find('table', {'id': 'currencies-all'}).find_all('tr')
#     coin_names = list()
#
#     for row in coin_rows:
#         name_tag = row.find('td', class_='no-wrap currency-name')
#         if name_tag is not None:
#             name = name_tag['data-sort']
#             symbol_tag = row.find('td', class_='text-left col-symbol').string
#             coin_names.append((name, symbol_tag))
#     return coin_names

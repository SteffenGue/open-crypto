import asyncio
import os

from bs4 import BeautifulSoup
from db_handler import DatabaseHandler
import urllib.request

from exchanges.exchange import Exchange
from tables import metadata
from utilities import read_config, yaml_loader


def get_coins() -> list:
    fp = urllib.request.urlopen("https://coinmarketcap.com/all/views/all/")
    soup = BeautifulSoup(fp, 'html.parser')
    fp.close()

    coin_rows = soup.find('table', {'id': 'currencies-all'}).find_all('tr')
    coin_names = list()

    for row in coin_rows:
        name_tag = row.find('td', class_='no-wrap currency-name')
        if name_tag is not None:
            name = name_tag['data-sort']
            symbol_tag = row.find('td', class_='text-left col-symbol').string
            coin_names.append((name, symbol_tag))
    return coin_names


async def main():
    db_params = read_config('database')
    database_handler = DatabaseHandler(metadata, **db_params)

    #TODO RENABLE IF STARTING FIRST TIME
    # database_handler.persist_currencies(get_coins())

    #Extracting all exchange coonfigs
    exchanges_list = os.listdir('resources/running_exchanges')
    exchange_names = [x.split(".")[0] for x in exchanges_list if ".yaml" in x]
    #exchange_names = ['vindax']
    exchange_names.sort()

    database_handler.persist_exchanges(exchange_names)

    exchanges = {exchange_name: Exchange(yaml_loader(exchange_name)) for exchange_name in exchange_names}

    responses = await asyncio.gather(*(exchanges[ex].request('ticker') for ex in exchanges))

    for response in responses:
        print('Response: {}'.format(response))
        if response:
            exchange = exchanges[response[0]]
            formatted_response = exchange.get_ticker(response)
            database_handler.persist_tickers(formatted_response)

if __name__== "__main__":
    asyncio.run(main())


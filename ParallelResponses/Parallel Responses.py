import urllib
import time
import asyncio
from bs4 import BeautifulSoup
from db_handler import DatabaseHandler
from tables import metadata
from utilities import read_config


def get_coins() -> list:
    fp = urllib.request.urlopen("https://coinmarketcap.com/all/views/all/")
    soup = BeautifulSoup(fp, 'html.parser')
    fp.close()

    coin_rows = soup.find('table', {'id': 'currencies-all'}).find_all('tr')
    coin_names = list()

    # coin_names.append(('A', 'A'))
    # coin_names.append(('B', 'B'))
    # # coin_names.append(('C', 'C'))
    # # coin_names.append(('D', 'D'))
    # # coin_names.append(('E', 'E'))
    # coin_names.append(('F', 'F'))
    # coin_names.append(('G', 'G'))
    # coin_names.append(('H', 'H'))
    #
    # return coin_names

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
    #database_handler.persist_exchanges(exchanges.keys())
    # TODO Reactivate
    #database_handler.persist_currencies(get_coins())

    # Findet jetzt in persist_currencies statt
    # currency_ids = database_handler.get_all_currency_ids()
    # currency_pairs = database_handler.generate_currency_pairs(currency_ids)
    # database_handler.persist_currency_pairs(currency_pairs)
    # database_handler.bulk_currency_pairs(currency_pairs)

    # while True:
    responses = await asyncio.gather(*(exchanges[ex].request('ticker') for ex in exchanges))

    formatted_tickers = []
    start = time.time()
    for response in responses:
        formatted_tickers.append(exchanges[response[0]].format_ticker(response[1], response[2]))
        #formatted_tickers = exchanges[response[0]].get_ticker(response[1], response[2])
        # await database_handler.persist_tickers(formatted_tickers)
    await asyncio.gather(*(database_handler.persist_tickers(ticker) for ticker in formatted_tickers))
    end = time.time()
    print(end-start)
    # time.sleep(10)


if __name__ == '__main__':
    asyncio.run(main())

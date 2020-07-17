import asyncio
from datetime import datetime, timedelta
from typing import Dict, List

from Job import Job
from scheduler import Scheduler
from db_handler import DatabaseHandler
from exchanges.exchange import Exchange
from tables import metadata, ExchangeCurrencyPair
from utilities import read_config, yaml_loader, get_exchange_names


def initialize_jobs(database_handler: DatabaseHandler, job_config: Dict) -> List[Job]:
    jobs: [Job] = list()
    for job in job_config.keys():
        job_params: Dict = job_config[job]

        exchanges_with_pairs: [Exchange, List[ExchangeCurrencyPair]] = dict()
        exchanges = {}
        for exchange_name in job_params['exchanges']:
            exchange: Exchange = Exchange(yaml_loader(exchange_name))
            exchange_currency_pairs: List[ExchangeCurrencyPair] = database_handler.collect_exchanges_currency_pairs(
                exchange.name,
                job_params['currency_pairs'],
                job_params['first_currency'],
                job_params['second_currency'])
            exchanges_with_pairs[exchange] = exchange_currency_pairs
            exchanges[exchange.name] = exchange

        new_job: Job = Job(job,
                           job_params['yaml_request_name'],
                           job_params['frequency'],
                           exchanges_with_pairs,
                           exchanges)
        jobs.append(new_job)
    return jobs


async def main(database_handler: DatabaseHandler, jobs: List[Job]):
    """
    The main() function to run the program. Loads the database, including the database_handler.
    The exchange_names are extracted with a helper method in utilities based on existing yaml-files.
    In an asynchronous manner it is iterated over the exchanges and and the responses are awaited and collected
        by await asyncio.gather(..)
    As soon as all responses from the exchanges are returned, the values get extracted, formatted into tuples
        by the exchange.get_ticker(..) method and persisted by the into the database by the database_handler.
    """

    # run program with single exchange for debugging/testing purposes
    # exchange_names = ['binance']
    # TODO nicht vergessen config path zu ändern: gerade in hr_exchanges
    sched = Scheduler(database_handler, jobs)
    for job in jobs:
        if job.request_name == 'ticker':
            await sched.get_tickers(job.exchanges_with_pairs)
        elif job.request_name == 'currency_pairs':
            await sched.get_currency_pairs(job.exchanges)
        elif job.request_name == 'historic_rates':
            await sched.get_historic_rates(job.exchanges)
        else:
            print('{} is not a supported job.'.format(job.request_name))

if __name__ == "__main__":
    db_params = read_config('database')
    database_handler = DatabaseHandler(metadata, **db_params)
    jobs = initialize_jobs(database_handler, read_config('jobs'))
    asyncio.run(main(database_handler, jobs))
    # scheduler = Scheduler(database_handler, jobs)
    # scheduler.run()

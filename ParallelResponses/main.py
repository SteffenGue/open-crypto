import asyncio
from typing import Dict, List

from model.scheduling.Job import Job
from model.scheduling.scheduler import Scheduler
from model.database.db_handler import DatabaseHandler
from model.exchange.exchange import Exchange
from model.database.tables import metadata, ExchangeCurrencyPair
from model.utilities.utilities import read_config, yaml_loader, get_exchange_names


async def initialize_jobs(database_handler: DatabaseHandler, job_config: Dict, update_cp=True) -> List[Job]:
    jobs: [Job] = list()
    for job in job_config.keys():
        job_params: Dict = job_config[job]

        exchanges_with_pairs: [Exchange, List[ExchangeCurrencyPair]] = dict()
        exchange_names = job_params['exchanges'] if job_params['exchanges'][0] != 'all' else get_exchange_names()

        for exchange_name in exchange_names:
            # TODO: Error, wenn yaml nicht existiert
            exchange: Exchange = Exchange(yaml_loader(exchange_name))

            if update_cp & ('currency_pairs' != job_params['yaml_request_name']):
                print("Updating Currency Pairs: {}".format(exchange_name))
                response = await exchange.request_currency_pairs('currency_pairs')
                if response[1] is not None:
                    formatted_response = exchange.format_currency_pairs(response)
                    database_handler.persist_exchange_currency_pairs(formatted_response)
            exchange_currency_pairs: List[ExchangeCurrencyPair] = database_handler.collect_exchanges_currency_pairs(
                exchange.name,
                job_params['currency_pairs'],
                job_params['first_currency'],
                job_params['second_currency'])
            exchanges_with_pairs[exchange] = exchange_currency_pairs

        new_job: Job = Job(job,
                           job_params['yaml_request_name'],
                           job_params['frequency'],
                           exchanges_with_pairs)
        jobs.append(new_job)
    return jobs


async def main(database_handler: DatabaseHandler):
    """
    The model() function to run the program. Loads the database, including the database_handler.
    The exchange_names are extracted with a helper method in utilities based on existing yaml-files.
    In an asynchronous manner it is iterated over the exchange and and the responses are awaited and collected
        by await asyncio.gather(..)
    As soon as all responses from the exchange are returned, the values get extracted, formatted into tuples
        by the exchange.get_ticker(..) method and persisted by the into the database by the database_handler.
    """
    # Falls du doch die Pairs brauchst
    # exchanges_to_update_currency_pairs_on: Dict[str, Exchange] = dict()
    # for job in jobs: job_exchanges: [Exchange] = job.exchanges_with_pairs.keys()
    #     for exchange in job_exchanges:
    #         if all(exchange.name != ex_to_update for ex_to_update in exchanges_to_update_currency_pairs_on):
    #             exchanges_to_update_currency_pairs_on[exchange.name] = exchange

    jobs = await initialize_jobs(database_handler, read_config('jobs'), read_config('updates')['update_currency_pairs'])
    sched = Scheduler(database_handler, jobs)
    timeout_in_minutes = None
    for j in jobs:
        timeout_in_minutes = j.frequency
    timeout_in_minutes *= 60
    while True:
        await asyncio.gather(sched.run(), asyncio.sleep(timeout_in_minutes))


if __name__ == "__main__":
    db_params = read_config('database')
    database_handler = DatabaseHandler(metadata, **db_params)
    asyncio.run(main(database_handler))
    # scheduler = Scheduler(database_handler, jobs)
    # scheduler.run()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TODO: Fill out module docstring.
"""

import asyncio
import json
import os
from typing import List

from model.database.db_handler import DatabaseHandler
from model.database.tables import metadata
from model.exchange.exchange import Exchange
from model.utilities.utilities import read_config, get_exchange_names, yaml_loader

job_name: str = "historic_rates"  # currency-pairs, historic-rates...
yaml_path: str = "../resources/running_exchanges/all/"
saving_path: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json_test_responses")

db_params = read_config("database")
database_handler = DatabaseHandler(metadata, **db_params)

exchange_names = get_exchange_names()

print(saving_path)
exchanges: List[Exchange] = list()
for exchange_name in exchange_names:
    exchanges.append(Exchange(yaml_loader(exchange_name), None, None))

for exchange in exchanges:
    # Making sure every exchange has all the available currency-pairs
    # pair_response = asyncio.run(exchange.request_currency_pairs('currency_pairs'))
    # pairs = exchange.format_currency_pairs(pair_response)
    # database_handler.persist_exchange_currency_pairs(pairs, True)

    all_currency_pairs_for_ex = database_handler.get_all_currency_pairs_from_exchange(exchange.name)
    responses = asyncio.run(exchange.request(job_name, all_currency_pairs_for_ex))[2]
    for currency_pair in responses.keys():
        full_saving_path = os.path.join(saving_path, exchange.name, job_name)
        if not os.path.exists(full_saving_path):
            os.makedirs(full_saving_path)
        if currency_pair:
            first_currency: str = currency_pair.first.name
            second_currency: str = currency_pair.second.name
            json_file = open(os.path.join(full_saving_path,
                                          f"{exchange.name.upper()}_{first_currency}_{second_currency}"),
                             "w", encoding="UTF-8")
        else:
            json_file = open(os.path.join(full_saving_path, f"{exchange.name.upper()}_ALL"), "w", encoding="UTF-8")

        json.dump(responses[currency_pair], json_file, indent=4)
        json_file.close()

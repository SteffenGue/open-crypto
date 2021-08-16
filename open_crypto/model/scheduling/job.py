#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module providing a job which defines the request method and exchanges currency-pairs.

Classes:
    - Job
"""
from typing import Any

from model.database.tables import ExchangeCurrencyPair
from model.exchange.exchange import Exchange


class Job:
    """
    Represents a job that is executed by the Scheduler based on the frequency set in config.yaml.
    A job itself is 'created' in config.yaml.
    Should only be used to hold information for it tasks.
    """

    def __init__(self,
                 name: str,
                 job_params: dict[str, Any],
                 exchanges_with_pairs: dict[Exchange, list[ExchangeCurrencyPair]]):
        """
        Initializer of a job.

        @param name: Name of the job taken out of config-file
        @type name: str
        @param job_params: All job parameter from the config-file
        @type job_params: dict
        @param exchanges_with_pairs: Dict for each exchange and it's currency pairs that have to be queried from it.
        @type exchanges_with_pairs: dict[Exchange, list[ExchangeCurrencyPair]]
        """
        self.name = name
        self.request_name = job_params["yaml_request_name"]
        self.exchanges_with_pairs = exchanges_with_pairs
        self.job_params = job_params

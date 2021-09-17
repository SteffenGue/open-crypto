#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module providing a job which defines the request method and exchanges currency-pairs.

Classes:
    - Job
"""
from typing import Any, Dict, Optional

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
                 job_params: Dict[str, Any],
                 exchanges_with_pairs: Dict[Exchange, Dict[ExchangeCurrencyPair, Optional[int]]]):
        """
        Initializer of a job.

        @param name: Name of the job taken out of config-file
        @type name: str
        @param job_params: All job parameter from the config-file
        @type job_params: dict
        @param exchanges_with_pairs: Dict for each exchange and it's currency pairs that have to be queried from it
                                     together with the last inserted row_id, if available..
        @type exchanges_with_pairs: dict[Exchange, Dict[ExchangeCurrencyPair], Optional[int]]
        """
        self.name = name
        self.request_name = job_params["request_method"]
        self.exchanges_with_pairs = exchanges_with_pairs
        self.job_params = job_params

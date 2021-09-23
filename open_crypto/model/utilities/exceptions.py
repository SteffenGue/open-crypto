#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module providing additional exception classes.

Classes:
    - MappingNotFoundException
    - DifferentExchangeContentException
    - NoCurrencyPairProvidedException
"""


class MappingNotFoundException(Exception):
    """
    Custom exception that is thrown when needed mappings could not be found. Most likely this happens when there is a
    typo in the exchange-config file or the file is incomplete.
    """

    def __init__(self, exchange_name: str, mapping_name: str):
        """
        @param exchange_name: Current exchange the exception is thrown for.
        @type exchange_name: str
        @param mapping_name: Name of the method/mappings that are needed.
        @type mapping_name: str
        """
        super().__init__(f"No mapping with the name '{mapping_name}' for the exchange '{exchange_name}' found.")


class DifferentExchangeContentException(Exception):
    """Custom exception that is thrown when the given content is from a different exchange."""

    def __init__(self, name_of_given_exchange: str, name_of_receiving_exchange: str):
        """
        @param name_of_given_exchange: Name of the exchange that the content is from.
        @type name_of_given_exchange: str
        @param name_of_receiving_exchange: Nome of the exchange that the content is handed to.
        @type name_of_receiving_exchange: str
        """
        super().__init__(f"'{name_of_receiving_exchange}' was given content from '{name_of_given_exchange}'.")


class NoCurrencyPairProvidedException(Exception):
    """
    Custom exception that is thrown when there was no currency_pair_first or _second found or provided in the
    exchange-config but is needed.
    """

    def __init__(self, exchange_name: str, method_name: str):
        """
        @param exchange_name: Name of the exchange that had no currency pair provided.
        @type exchange_name: str
        @param method_name: Name of the method where the currency pair is needed.
        @type method_name: str
        """
        super().__init__(f"For the exchange '{exchange_name}' for the method '{method_name}' currency pair info is"
                         f"needed but not sufficiently provided.")

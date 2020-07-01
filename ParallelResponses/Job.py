from typing import Dict

from exchanges.exchange import Exchange
from utilities import yaml_loader


class Job:
    name: str
    request_name: str
    frequency: int  # TODO: irgendwie klar machen, dass es auch um minutes geht
    exchanges: [Exchange]
    currency_pairs: [(str, str)]
    first_currencies: [str]
    second_currencies: [str]

    def __init__(self, name: str,
                 request_name: str,
                 frequency: int,
                 exchange_names: [Exchange],
                 currency_pairs: [Dict[str, str]],
                 first_currencies: [str],
                 second_currencies: [str]):
        self.name = name
        self.request_name = request_name
        self.frequency = frequency
        self.exchanges = {exchange_name: Exchange(yaml_loader(exchange_name))
                          for exchange_name in exchange_names}
        self.currency_pairs = currency_pairs
        self.first_currencies = first_currencies
        self.second_currencies = second_currencies

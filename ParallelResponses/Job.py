from typing import Dict, List

from exchanges.exchange import Exchange
from tables import ExchangeCurrencyPair
from utilities import yaml_loader


class Job:
    name: str
    request_name: str
    frequency: int  # TODO: irgendwie klar machen, dass es auch um minutes geht
    exchanges_with_pairs: [Exchange, [ExchangeCurrencyPair]]

    def __init__(self,
                 name: str,
                 request_name: str,
                 frequency: int,
                 exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]]):
        self.name = name
        self.request_name = request_name
        self.frequency = frequency
        self.exchanges_with_pairs = exchanges_with_pairs



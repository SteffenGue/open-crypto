from typing import Dict, List

from model.exchange.exchange import Exchange
from model.database.tables import ExchangeCurrencyPair


class Job:
    name: str
    request_name: str
    exchanges_with_pairs: [Exchange, [ExchangeCurrencyPair]]

    def __init__(self,
                 name: str,
                 request_name: str,
                 exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]]):
        self.name = name
        self.request_name = request_name
        self.exchanges_with_pairs = exchanges_with_pairs



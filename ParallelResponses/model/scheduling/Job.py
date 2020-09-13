from typing import Dict, List

from model.exchange.exchange import Exchange
from model.database.tables import ExchangeCurrencyPair


class Job:
    """
    Represents a job that is executed by the Scheduler based on the frequency set in config.yaml.
    A job itself is 'created' in config.yaml.
    Should only be used to hold information for it tasks.
    """
    name: str
    request_name: str
    exchanges_with_pairs: [Exchange, [ExchangeCurrencyPair]]

    def __init__(self,
                 name: str,
                 request_name: str,
                 exchanges_with_pairs: Dict[Exchange, List[ExchangeCurrencyPair]]):
        """
        Initializer of a job.

        @param name:
            Name of the job taken out of config.yaml
        @param request_name:
            Name of the request name taken out of config.yaml
        @param exchanges_with_pairs:
            Dictionary for each exchange and it's currency pairs that have to be queried from it.
        """
        self.name = name
        self.request_name = request_name
        self.exchanges_with_pairs = exchanges_with_pairs



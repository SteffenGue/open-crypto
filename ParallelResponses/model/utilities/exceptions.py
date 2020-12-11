class MappingNotFoundException(Exception):
    """ Custom exception that is thrown when needed mappings could not be found.
        Most likely this happens when there is a typo in the exchange-config file or
        the file is incomplete."""
    def __init__(self, exchange_name: str, mapping_name: str):
        """
        @param exchange_name: Current exchange the exception is thrown for.
        @param mapping_name: Name of the method/mappings that are needed.
        """
        Exception.__init__(self, 'No mapping with the name \'{}\' for the exchange \'{}\' found.'.format(mapping_name, exchange_name))


class DifferentExchangeContentException(Exception):
    """ Custom exception that is thrown when the given content is from a different exchange."""
    def __init__(self, name_of_given_exchange: str, name_of_receiving_exchange: str):
        """
        @param name_of_given_exchange: Name of the exchange that the content is from.
        @param name_of_receiving_exchange: Nome of the exchange that the content is handed to.
        """
        Exception.__init__(self, '\'{}\' was given content from \'{}\'.'.format(name_of_receiving_exchange, name_of_given_exchange))


class NoCurrencyPairProvidedException(Exception):
    """ Custom exception that is thrown when there was no currency_pair_first or _second found or provided
        in the exchange-config but is needed."""
    def __init__(self, exchange_name: str, method_name: str):
        """

        @param exchange_name: Name of the exchange that had no currency pair provided.
        @param method_name: Name of the method where the currency pair is needed
        """
        Exception.__init__(self, 'For the exchange \'{}\' for the method \'{}\' currency pair info is needed but not sufficiently provided.'.format(exchange_name, method_name))

class NotAllPrimaryKeysException(Exception):
    """Custom exception indicating that not all primary keys are in the formatted_response. This exception is raised
        prior to any database interaction."""

    def __init__(self, exchange_name: str, pkeys: dict):
        """
        :param exchange_name: Name of the Exchange that had a missing primary key.
        :param pkeys: String representation of all missing primary keys
        """
        Exception.__init__(self, "The exchange \'{}\' is missing a primary key: \'{}\'".format(exchange_name, pkeys))

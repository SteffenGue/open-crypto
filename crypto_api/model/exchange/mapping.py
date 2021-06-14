#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TODO: Fill out module docstring.
"""

from collections import deque
from collections.abc import Iterable
from typing import Collection, Optional

from model.utilities.utilities import TYPE_CONVERSIONS


def convert_type(value, types_queue: deque):
    """
    Converts the value via type conversions.

    Helper method to convert the given value via a queue of type conversions.

    @param value: The value to get converted to another type.
    @type value: Any
    @param types_queue: The queue of type conversion instructions.
    @type types_queue: deque

    @return: The converted value.
    @rtype: Any
    """
    current_type = types_queue.popleft()

    result = value

    while types_queue:
        next_type = types_queue.popleft()

        types_tuple = (current_type, next_type)

        if "continue" in types_tuple:
            continue

        conversion = TYPE_CONVERSIONS[types_tuple]

        params = list()

        for _ in range(conversion["params"]):
            params.append(types_queue.popleft())

        # Change here to avoid "None" as result value in the params when no value to convert is needed (i.e. when
        # methods are called with ("none", ...).
        # if not result and isinstance(result, (str, list)):
        try:
            if result is None:
                result = conversion["function"](*params)
            else:
                result = conversion["function"](result, *params)

            current_type = next_type
        except Exception:
            return None

    return result


class Mapping:
    """
    Class representing mapping data and logic.

    Class representing mapping data und further functionality provided
    with methods.

    Attributes:
        key:
            String being the keyword indicating one or several
            database table columns. See "database_column_mapping"
            in "config.yaml".
        path:
            An ordered list of keys used for traversal through the
            response dict with the intention of returning the value wanted
            for the database.
        types:
            An ordered sequence of types and
            additional parameters (if necessary). Is used to conduct
            type conversions within the method "extract_value()".
    """

    def __init__(self,
                 key: str,
                 path: list,
                 types: list):
        """
        Constructor of Mapping.

        Constructor method for constructing method objects.

        @param key: String being the keyword indicating one or several
                    database table columns. See "database_column_mapping"
                    in "config.yaml".
        @type key: str
        @param path: An ordered list of keys used for traversal through the
                     response dict with the intention of returning the value wanted
                     for the database.
        @type path: list
        @param types: An ordered sequence of types and
                      additional parameters (if necessary). Is used to conduct
                      type conversions within the method "extract_value()".
        @type types: list
        """
        self.key = key
        self.path = path
        self.types = types

    def traverse_path(self, response: dict, path_queue: deque, currency_pair_info: tuple[str, str, str] = None) \
            -> Optional[dict]:
        """
        Traverses the path on a response.

        Helper method for traversing the path on the given response dict (subset).

        @param response: The response dict (subset).
        @type response: dict
        @param path_queue: The queue of path traversal instructions.
        @type path_queue: deque
        @param currency_pair_info: The formatted String of a currency pair.
                                   For special case that the key of a dictionary is the formatted currency pair string.
        @type currency_pair_info: tuple[str, str, str]

        @return: The traversed response dict.
        @rtype: Optional[dict]
        """
        path_element = path_queue.popleft()

        if path_element == "dict_key":
            # Special case to extract value from "dict_key"
            traversed = list(response.keys())
        elif path_element == "dict_values":
            # Special case to extract value from "dict_values"
            traversed = list(response.values())
        elif path_element == "list_key":
            # Special case with the currency_pair prior to a list
            traversed = list(response.keys())
        elif path_element == "list_values":
            traversed = list(response.values())
        elif path_element == []:
            # Special case to extract multiple values from a single list ["USD","BTC",...]
            traversed = response
        elif path_element == "currency_pair" and currency_pair_info[2] is not None:
            traversed = response[currency_pair_info[2]]
        elif is_scalar(response):
            return None
        else:  # Hier editiert für Kraken sonderfall
            if isinstance(response, dict) and path_element not in response.keys():
                return None
            else:
                traversed = response[path_element]

        return traversed

    def extract_value(self,
                      response: Collection,
                      path_queue: deque = None,
                      types_queue=None,
                      iterate=True,
                      currency_pair_info: tuple[str, str, str] = (None, None, None)):
        """
        Extracts the value specified by "self.path".

        Extracts the value specified by the path sequence and converts it
        using the "types" specified.

        @param response: The response dict (JSON) returned by an API request.
        @type response: Collection
        @param path_queue: The queue of path traversal instructions.
        @type path_queue: deque
        @param types_queue: The queue of type conversion instructions.
        @type types_queue: deque
        @param iterate: Whether still an auto-iteration is possible.
        @type iterate: bool
        @param currency_pair_info: The formatted String of a currency pair.
        @type currency_pair_info: tuple[str, str, str]

        @return: The value specified by "path_queue" and converted
                 using "types_queue".
                 Can be a list of values which get extracted iteratively from
                 the response.
        @rtype: Any
        """
        if path_queue is None:
            path_queue = deque(self.path)

        if types_queue is None:
            types_queue = deque(self.types)

        if not response:
            return None

        if not path_queue:
            # TODO: after integration tests, look if clause for first and second currency can be deleted!
            if types_queue[0] == "first_currency":
                return currency_pair_info[0]
            elif types_queue[0] == "second_currency":
                return currency_pair_info[1]
            return convert_type(None, types_queue)

        while path_queue:

            if iterate and isinstance(response, list):
                # Iterate through list of results
                result = list()

                # special case for bitfinex, der ganz lange auskommentiert war -> ganzes if, else war entsprechend einen nach links gerürckt
                if len(response) == 1:
                    response = response[0]
                    continue  # because instance of response has to be checked

                for item in response:

                    if is_scalar(item):
                        return self.extract_value(response,
                                                  path_queue,
                                                  types_queue,
                                                  iterate=False)

                    result.append(
                        self.extract_value(
                            item,
                            deque(path_queue),
                            deque(types_queue)
                        )
                    )

                return result
                # bis hier war in else

            elif is_scalar(response):
                # Return converted scalar value
                return convert_type(response, types_queue)

            # Zusätzlich eingefügt für den Fall eines leeren Dict/List. Hier wurden bei Bitz 0.0 zurückgegeben,
            # was im Folgenden zu einem Integrity-Error beim persisiteren geführt hat.
            elif not response:
                return None

            else:
                # Traverse path
                response = self.traverse_path(response, path_queue, currency_pair_info=currency_pair_info)

        if types_queue and response is not None:  # hier zu None geändert, weil sonst nicht zu 0 zo Bool geändert werden kann

            if isinstance(response, list):

                result = list()

                for item in response:
                    result.append(
                        convert_type(item, deque(types_queue))
                    )

                # for dict_key special_case aka.  test_extract_value_list_containing_dict_where_key_is_value() in test_mapping.py
                if len(result) == 1:
                    result = result[0]

                response = result

            else:
                response = convert_type(response, types_queue)

        return response

    def __str__(self) -> str:
        """String representation of a Mapping"""
        string_path = list()

        for item in self.path:
            string_path.append(str(item))

        return " / ".join(string_path) + " -> " + str(self.key)


def is_scalar(value) -> bool:
    """
    Indicates whether a value is a scalar or not.

    Convenience function returning a bool whether the provided value is a single value or not.
    Strings count as scalar although they are iterable.

    @param value: The value to evaluate concerning whether it is a single value
                  or multiple values (iterable).
    @type value: Any

    @return: Bool indicating whether the provided value is a single value or not.
    @rtype: bool
    """
    # TODO: Philipp: Check if return isinstance(value, Iterator) works
    return isinstance(value, str) or not isinstance(value, Iterable)

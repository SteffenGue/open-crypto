#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Contains exceptions to represent validation failures.
"""

from typing import Text, Any, Iterable, Union, Type, Dict, List

import validators


class ValidationError(Exception):
    """
    Exception in case a validated value is not valid.
    """


class KeyNotInDictError(ValidationError):
    """
    Exception in case a required key is not included in a particular dict.

    Attributes:
        missing_key:
            A key that is not included in a dict.
        inspected_dict:
            A dict in which a certain key should be included.
    """

    def __init__(self, missing_key: Text, inspected_dict: Dict[Text, Any]):
        """
        Constructor of KeyNotInDictError.

        @param missing_key: A key that is not included in a dict.
        @param inspected_dict: A dict in which a certain key should be included.
        """
        super().__init__("Key was not in Dict.")
        self.missing_key = missing_key
        self.inspected_dict = inspected_dict

    def __str__(self) -> Text:
        """
        A method for representing a text.

        A text value returning the missing key, which is no part of the
        inspected dict.

        @return: A Text.
        """
        return f"Key '{self.missing_key}' not in keys {list(self.inspected_dict.keys())}"


class KeyNotIntendedError(ValidationError):
    """
    Exception in case that a key is not intended to be in a certain dict.

    Attributes:
        intended_keys:
            A set of keys, which are intended to be a part of a dict.
        actual_key:
            A key, which is not intended to be in a dict.
    """

    def __init__(self, intended_keys: Iterable[Text], actual_key: Text):
        """
        Constructor of KeyNotIntendedError.

        @param intended_keys: A set of keys, which are intended to be a part of a dict.
        @param actual_key: A key, which is not intended to be in a dict.
        """
        super().__init__("Key was not intended to be in Dict.")
        self.intended_keys = set(intended_keys)
        self.actual_key = actual_key

    def __str__(self) -> Text:
        """
        A method for representing a text.

        A text value returning the actual key, which is not intended to be part
        of a certain dict and the intended keys, which are allowed to be in the
        dict.

        @return: A Text.
        """
        return f"Key '{self.actual_key}' not intended to be in Dict, Allowed: {self.intended_keys}."


class SubstringNotInStringError(ValidationError):
    """
    Exception in case that a substring is not part of a certain string.

    Attributes:
        missing_substring:
            A substring, which is no a part of a string.
        inspected_string:
            A string, in which the substring shall be contained.
    """

    def __init__(self, missing_substring: Text, inspected_string: Text):
        """
        Constructor of SubstringNotInStringError.

        @param missing_substring: A substring, which is not a part of a string.
        @param inspected_string: A string, in which the substring shall be contained.
        """
        super().__init__("Expected Substring was not found in String.")
        self.missing_substring = missing_substring
        self.inspected_string = inspected_string

    def __str__(self) -> Text:
        """
        A method for representing a text.

        A text value returning the missing substring, which is not a part of
        the inspected string.

        @return: A Text.
        """
        return f"Substring '{self.missing_substring}' not in '{self.inspected_string}'"


class WrongTypeError(ValidationError):
    """
    Exception in case that a value has a wrong type.

    Attributes:
        expected_type:
            A type that is expected.
        actual_type:
            The actual type, which is not the expected type.
    """

    def __init__(
            self,
            expected_type: Union[Type, Iterable[Type]],
            actual_type: Type,
            key: str = None):
        """
        Constructor of WrongTypeError.

        @param expected_type: A type that is expected.
        @param actual_type: The actual type, which is not the expected type.
        """
        super().__init__("Value has wrong type.")
        self.expected_type = set(expected_type) if isinstance(expected_type, Iterable) else expected_type
        self.actual_type = actual_type
        self.key = key

    def __str__(self) -> Text:
        """
        A method for representing a text.

        A text value returning the expected type(s), which is/are unlike the
        actual type.

        @return: A Text.
        """
        return f"{'Key ' + self.key + ': ' if self.key else ''}Expected type(s) '{self.expected_type}' " \
               f"!= actual type '{self.actual_type}'."


class UrlValidationError(ValidationError):
    """
    Exception in case that a URL is not valid.
    """

    def __init__(self, url: Text, report: validators.ValidationFailure = None):
        """
        Constructor of UrlValidationError.

        @param url: A URL that shall be checked.
        @param report: The report when a URL is not valid.
        """
        super().__init__("URL was not valid.")
        self.url = url
        self.report = report

    def __str__(self) -> Text:
        """
        A method for representing a text.

        A text value returning the invalid URL.

        @return: A Text.
        """
        return f"URL '{self.url}' is not valid."


class NamingConventionError(ValidationError):
    """
    Exception in case that the naming convention is violated.
    """

    def __init__(self, naming_pattern: Text, name: Text):
        """
        Constructor of NamingConventionError.

        @param naming_pattern: A given naming pattern.
        @param name: The name which shall be checked.
        """
        super().__init__("Naming convention was violated.")
        self.naming_pattern = naming_pattern
        self.name = name

    def __str__(self) -> Text:
        """
        A method for representing a text.

        A text value returning the name, which does not match the naming
        convention.

        @return: A Text.
        """
        return f"'{self.name}' did not match naming convention's Regex Pattern '{self.naming_pattern}'."


class WrongValueError(ValidationError):
    """
    Exception in case that a value has a wrong type.
    """

    def __init__(
            self,
            expected_value: List[Any],
            actual_value: Union[str, int, float],
            key: str):
        """
        Constructor of WrongValueError.

        Args:
            expected_value:
                A value that is expected.
            actual_value:
                The actual value, which is not the expected value.
        """
        super().__init__("Key has wrong value.")
        self.expected_value = expected_value \
            if not isinstance(expected_value, Iterable) else set(expected_value)
        self.actual_value = actual_value
        self.key = key

    def __str__(self) -> Text:
        """A method for representing a text.

        A text value returning the expected value(s), which is/are unlike the
        actual value.

        @return: A Text.
        """
        return f"Expected value(s) '{self.expected_value}' != actual value '{self.actual_value}' in '{self.key}'."


class WrongCompositeValueError(ValidationError):
    """
    Exception in case that a composition of values have the wrong type.

    Attributes:
        keys:
            The keys containing the wrong values.

    """

    def __init__(self, keys: List[Union[str, Type]]):
        """
        Constructor of WrongCompositeValueError.

        Args:
            keys:
                A type that is expected.
        """
        super().__init__("Too many None values.")
        self.keys = keys

    def __str__(self) -> Text:
        """A method for representing a text.

        A text value returning the expected value(s), which is/are unlike the
        expected value.

        @return: A Text.
        """
        return f"Expected one key(s) '{self.keys}' != None."


class WrongCurrencyPairFormatError(ValidationError):
    """
    Exception in case that a value has a wrong type.
    """

    def __init__(
            self,
            expected_value: List[str],
            actual_value: Union[Any],
            key: str):
        """
        Constructor of WrongCurrencyPairFormatError.

        Args:
            expected_value:
                A value that is expected.
            actual_value:
                The actual value, which is not the expected value.
            key:
                The incorrectly specified key.
        """
        super().__init__("Key has wrong value.")
        self.expected_value = expected_value
        self.actual_value = actual_value
        self.key = key

    def __str__(self) -> Text:
        """A method for representing a text.

        A text value returning the expected value(s), which is/are unlike the
        actual value.

        @return: A Text.
        """
        return f"Expected splitting value(s) '{self.expected_value}' != actual value '{self.actual_value}' " \
               f"in '{self.key}'."


class CustomBaseExceptionError(ValidationError):
    """
    Custom base exception
    """

    def __init__(self, key: Any, msg: Text):
        """
        Constructor of WrongCurrencyPairFormatError.

        @param key:
            The incorrectly specified key.
        @param msg:
            The exception message to return.
        """
        super().__init__("Custom base exception raised.")
        self.key = key
        self.msg = msg

    def __str__(self) -> Text:
        """A method for representing a text.

        A text value returning the expected value(s), which is/are unlike the
        actual value.

        @return: A Text.
        """
        return f"Error in key '{self.key}': {self.msg}."

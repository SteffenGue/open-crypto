#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Contains exceptions to represent validation failures.
"""

from typing import Text, Any, Iterable, Union, Type

import validators


class ValidationError(Exception):
    """
    Exception in case that the validation failed.
    """


class KeyNotInDictError(ValidationError):
    """
    Exception in case that a key is not contained in a certain dict.

    Attributes:
        missing_key:
            A key, that is no part of a dict.
        inspected_dict:
            A dict, in which a certain key shall be contained.
    """
    missing_key: Text
    inspected_dict: dict[Text, Any]

    def __init__(self, missing_key: Text, inspected_dict: dict[Text, Any]):
        """
        Constructor of KeyNotInDictError.

        Args:
            missing_key:
                A key, that is no part of a dict.
            inspected_dict:
                A dict, in which a certain key shall be contained.
        """
        super().__init__("Key was not in Dict.")
        self.missing_key = missing_key
        self.inspected_dict = inspected_dict

    def __repr__(self) -> Text:
        """
        A method for representing a text.

        A text value returning the missing key, which is no part of the
        inspected dict.

        Returns:
            A Text.
        """
        return f"Key {repr(self.missing_key)} not in keys {repr(list(self.inspected_dict.keys()))}"


class KeyNotIntendedError(ValidationError):
    """
    Exception in case that a key is not intended to be in a certain dict.

    Attributes:
        intended_keys:
            A set of keys, which are intended to be a part of a dict.
        actual_key:
            A key, which is not intended to be in a dict.
    """
    intended_keys: set[Text]
    actual_key: Text

    def __init__(self, intended_keys: Iterable[Text], actual_key: Text):
        """
        Constructor of KeyNotIntendedError.

        Args:
            intended_keys:
                A set of keys, which are intended to be a part of a dict.
            actual_key:
                A key, which is not intended to be in a dict.
        """
        super().__init__("Key was not intended to be in Dict.")
        self.intended_keys = set(intended_keys)
        self.actual_key = actual_key

    def __repr__(self) -> Text:
        """
        A method for representing a text.

        A text value returning the actual key, which is not intended to be part
        of a certain dict and the intended keys, which are allowed to be in the
        dict.

        Returns:
            A Text.
        """
        return f"Key {self.actual_key} not intended to be in Dict, Allowed: {self.intended_keys}."


class SubstringNotInStringError(ValidationError):
    """
    Exception in case that a substring is not part of a certain string.

    Attributes:
        missing_substring:
            A substring, which is no a part of a string.
        inspected_string:
            A string, in which the substring shall be contained.
    """
    missing_substring: Text
    inspected_string: Text

    def __init__(self, missing_substring: Text, inspected_string: Text):
        """
        Constructor of SubstringNotInStringError.

        Args:
            missing_substring:
                A substring, which is not a part of a string.
            inspected_string:
                A string, in which the substring shall be contained.
        """
        super().__init__("Expected Substring was not found in String.")
        self.missing_substring = missing_substring
        self.inspected_string = inspected_string

    def __repr__(self) -> Text:
        """
        A method for representing a text.

        A text value returning the missing substring, which is not a part of
        the inspected string.

        Returns:
            A Text.
        """
        return f"Substring {repr(self.missing_substring)} not in {repr(self.inspected_string)}"


class WrongTypeError(ValidationError):
    """
    Exception in case that a value has a wrong type.

    Attributes:
        expected_type:
            A type that is expected.
        actual_type:
            The actual type, which is not the expected type.
    """
    expected_type: Union[Type, Iterable[Type]]
    actual_type: Type

    def __init__(
            self,
            expected_type: Union[Type, Iterable[Type]],
            actual_type: Type):
        """
        Constructor of WrongTypeError.

        Args:
            expected_type:
                A type that is expected.
            actual_type:
                The actual type, which is not the expected type.
        """
        super().__init__("Value has wrong type.")
        self.expected_type = expected_type \
            if not isinstance(expected_type, Iterable) else set(expected_type)
        self.actual_type = actual_type

    def __repr__(self) -> Text:
        """A method for representing a text.

        A text value returning the expected type(s), which is/are unlike the
        actual type.

        Returns:
            A Text.
        """
        return "Expected type(s) {expected} != actual type {actual}.".format(
            expected=repr(self.expected_type),
            actual=repr(self.actual_type)
        )


class UrlValidationError(ValidationError):
    """Exception in case that a URL is not valid.

    """

    def __init__(self, url: Text, report: validators.ValidationFailure = None):
        """Constructor of UrlValidationError.

        Args:
            url:
                A URL that shall be checked.
            report:
                The report when a URL is not valid.
        """

        super().__init__("URL was not valid.")
        self.url = url
        self.report = report

    def __repr__(self) -> Text:
        """A method for representing a text.

        A text value returning the invalid URL.

        Returns:
            A Text.
        """
        return f"URL {repr(self.url)} is not valid."


class NamingConventionError(ValidationError):
    """Exception in case that the naming convention is violated.

    """

    def __init__(self, naming_pattern: Text, name: Text):
        """Constructor of NamingConventionError.

        Args:
            naming_pattern:
                A given naming pattern.
            name:
                The name which shall be checked.
        """

        super().__init__("Naming convention was violated.")
        self.naming_pattern = naming_pattern
        self.name = name

    def __repr__(self) -> Text:
        """A method for representing a text.

        A text value returning the name, which does not match the naming
        convention.

        Returns:
            A Text.
        """
        return f"'{self.name}' did not match naming convention's Regex Pattern {self.naming_pattern}."

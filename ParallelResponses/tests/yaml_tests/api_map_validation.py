# -*- coding: utf-8 -*-

# pylint: disable=too-many-lines

"""
Exchange API Map Validation

Classes for validating the API yaml files of Exchanges and generating
a complete Report.

general structure:
( validate.py calls ApiMapValidator )
-ApiMapValidator line 689 (calls)
    -LoadFileValidator
    -LoadYamlValidator
    -ApiMapValidator (contains)
        -NameValidator
        -ApiUrlValidator (calls)
            -UrlValidator
        -RateLimitValidator
        -RequestsValidator (calls)
            -ApiMethodValidator (contains)
                -RequestValidator (contains)
                    -TemplateValidator
                    -PairTemplateValidator
                    -ParamsValidator (calls)
                        -ParamValidator
                -ResponseValidator
                -MappingValidator (calls)
                    -MappingEntryValidator (contains)
                        -KeyValidator
                        -PathValidator
                        -TypeValidator

Authors:
    Carolina Keil
    Martin Schorfmann
    Paul Böttger

Since:
    19.03.2019

Version:
    26.04.2019
"""

import abc
import re
import textwrap
from typing import Any, Dict, Iterable, List, Set, Text, Type, Union
import validators
import oyaml as yaml


class Valid:
    """Wrapper class for Exception or Text

    Wrapper class for wrapping Exceptions or Text while providing a
    boolean value (False for Exceptions, True otherwise).

    Attributes:
        message:
            An Exception instance or a Text message (str).
    """

    message: Union[Exception, Text]

    def __init__(self, message: Union[Exception, Text]):
        """Constructor of Valid.

        Constructor method for creating Valid objects wrapping
        an Exception or Text message.

        Args:
            message:
                An Exception instance or a Text message (str).
        """
        self.message = message

    def __bool__(self) -> bool:
        """A boolean value.

        A boolean value returning False for Exceptions and True otherwise.

        Returns:
            False for Exceptions, True otherwise.
        """
        return not isinstance(self.message, Exception)

    def __repr__(self) -> Text:
        """A method for representing a message in text format.

        A text value returning "-" and the respective message, if message is a
        Exception, otherwise "+" and the respective message.

        Returns:
            "-" for Exceptions, "+" otherwise.
        """
        sign = "+" if self else "-"
        return sign + " " + repr(self.message)


class Report:
    """Report of Validation.

    Report of the Validation carried out in a Validator.
    Contains a list of Exceptions and Messages collected during Validation.

    Attributes:
        messages:
            List of wrapped Exceptions or Messages.
    """

    messages: List[Valid]

    def __init__(self, *messages):
        """Constructor of Report.

        Args:
            *messages:
                A variable-length sequence of Valid objects.
        """

        self.messages = list(messages)

    def indented_report(self) -> Text:
        """Indents the report.

        Indents the generated report representation according to the
        opening and closing brackets.

        Returns:
            The indented representation
        """

        report = repr(self)

        report = re.sub(r"\[", "[\n", report)
        report = re.sub(r",", ",\n", report)
        report = re.sub(r"]", "\n]", report)

        indented_lines = list()
        level = 0

        for line in report.splitlines():
            line = line.strip()

            if "]" in line:
                level -= 1

            indented_line = textwrap.indent(line, level * "  ")
            indented_lines.append(indented_line)

            if "[" in line:
                level += 1

        return "\n".join(indented_lines)

    def print_report(self):
        """Prints the Report's indented representation."""

        print(self.indented_report())

    def __bool__(self) -> bool:
        """A boolean value.

        A boolean value returning True if all elements in messages are True,
        otherwise returning False.

        Returns:
            True if all messages are True, False otherwise.
        """
        return all(self.messages)

    def __repr__(self) -> Text:
        """A method for representing a message in text format.

        A text value returning "-" and the respective message, if message is a
        Exception, otherwise "+" and the respective message.

        Returns:
            "-" for Exceptions, "+" otherwise.
        """
        if len(self.messages) == 1:
            return repr(self.messages[0])

        sign = "+" if self else "-"
        report = sign + " " + repr(self.messages)

        return report


class CompositeReport(Report):
    """CompositeReport consisting of multiple Reports.

    Attributes:
        reports:
            A list of child Reports.
    """

    reports: List[Report]

    def __init__(self, *reports):
        """Constructor of CompositeReport.

        Args:
            *reports:
                A variable-length sequence of Report objects.
        """

        super().__init__()

        self.reports = list(reports)

    def append_report(self, report: Union[Report, "Validator"]):
        """Appends a Report to reports.

        Appends a Report or a Validator's Report to children reports.

        Args:
            report:
                A Report or Validator containing a Report.
        """

        # if the parameter 'report' is an instance of Validator
        if isinstance(report, Validator):
            report = report.report

        self.reports.append(report)

    def __bool__(self) -> bool:
        """A boolean value.

        A boolean value returning True if all elements in reports are True,
        otherwise returning False.

        Returns:
            True if all reports are True, False otherwise.
        """
        return all(self.reports)

    def __repr__(self) -> Text:
        """A method for representing a report in text format.

        A text value returning "-" and the respective report, if report is a
        Exception, otherwise "+" and the respective report.

        Returns:
            "-" for Exceptions, "+" otherwise.
        """
        sign = "+" if self else "-"
        report = sign + " " + repr(self.reports)
        return report


class ValidationError(Exception):
    """Exception in case that the validation failed."""


class KeyNotInDictError(ValidationError):
    """Exception in case that a key is not contained in a certain dict.

    Attributes:
        missing_key:
            A key, that is no part of a dict.
        inspected_dict:
            A dict, in which a certain key shall be contained.
    """

    missing_key: Text
    inspected_dict: Dict[Text, Any]

    def __init__(self, missing_key: Text, inspected_dict: Dict[Text, Any]):
        """Constructor of KeyNotInDictError.

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
        """A method for representing a text.

        A text value returning the missing key, which is no part of the
        inspected dict.

        Returns:
            A Text.
        """

        return "Key {missing_key} not in keys {keys}".format(
            missing_key=repr(self.missing_key),
            keys=repr(list(self.inspected_dict.keys()))
        )


class KeyNotIntendedError(ValidationError):
    """Exception in case that a key is not intended to be in a certain dict.

    Attributes:
        intended_keys:
            A set of keys, which are intended to be a part of a dict.
        actual_key:
            A key, which is not intended to be in a dict.
    """

    intended_keys: Set[Text]
    actual_key: Text

    def __init__(self, intended_keys: Iterable[Text], actual_key: Text):
        """Constructor of KeyNotIntendedError.

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
        """A method for representing a text.

        A text value returning the actual key, which is not intended to be part
        of a certain dict and the intended keys, which are allowed to be in the
        dict.

        Returns:
            A Text.
        """
        return ("Key {actual} not intended to be in Dict, "
                "Allowed: {intended}.").format(actual=self.actual_key,
                                               intended=self.intended_keys)


class SubstringNotInStringError(ValidationError):
    """Exception in case that a substring is not part of a certain string.

    Attributes:
        missing_substring:
            A substring, which is no a part of a string.
        inspected_string:
            A string, in which the substring shall be contained.
    """

    missing_substring: Text
    inspected_string: Text

    def __init__(self, missing_substring: Text, inspected_string: Text):
        """Constructor of SubstringNotInStringError.

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
        """A method for representing a text.

        A text value returning the missing substring, which is not a part of
        the inspected string.

        Returns:
            A Text.
        """
        return (
            "Substring {missing_substring} not in {inspected_string}"
        ).format(
            missing_substring=repr(self.missing_substring),
            inspected_string=repr(self.inspected_string)
        )


class WrongTypeError(ValidationError):
    """Exception in case that a value has a wrong type.

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
        """Constructor of WrongTypeError.

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
        return "URL {url} is not valid.".format(url=repr(self.url))


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
        return ("'{name}' did not match naming convention's Regex Pattern "
                "{pattern}.").format(pattern=self.naming_pattern,
                                     name=self.name)


class Validator(abc.ABC):
    """Validator Base Class.

    Class for validating the value used in instantiation.

    Attributes:
        value:
            The value to get validated.
        report:
            The report created during validation.
    """

    value: Any
    report: Report

    def __init__(self, value: Union[Any, "Validator"]):
        """Constructor of Validator Base class.

        Args:
            value:
                The value to get validated or a validator having the
                result value.
        """

        # if the parameter 'value' is an instance of Validator
        if isinstance(value, Validator):
            self.value = value.get_result_value()
        else:
            self.value = value

        self.report = None

    def get_result_value(self) -> Any:
        """Returns the value of the validator.

        Returns:
            The value of the Validator.
        """
        return self.value

    @abc.abstractmethod
    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        raise NotImplementedError

    def __bool__(self) -> bool:
        """Returns the boolean value of its validation report."""

        return bool(self.report)


class CompositeValidator(Validator):
    """Composite Validator.

    CompositeValidator containing a list of Validators while being
    a Validator itself.

    Attributes:
        validators:
            A List of children Validators.
    """

    validators: List[Validator]

    def __init__(self, value: Any, *child_validators):
        """Constructor of CompositeValidator.

        Args:
            value:
                The value to get validated.
            *validators:
                A variable-length sequence of children Validators.
        """

        super().__init__(value)

        self.validators = list(child_validators)
        self.report = CompositeReport()

    def append_validator(self, validator: Validator):
        """Appends a validator to validators.

        Args:
            validator:
                The validator to get appended to list of validators.
        """

        self.validators.append(validator)

    def append_report(self, report: Union[Validator, Report]):
        """Appends a Report.

        Appends a Report or the Report of a Validator to the CompositeReport.

        Args:
            report:
                A Validator whose Report will get appended or a Report.
        """

        self.report.append_report(report)

    def get_result_value(self) -> Any:
        """Returns the value or result.

        Returns its value if no special Validator is the last of its children.
        Otherwise the result value of the last Validator is returned.

        Returns:
            The value or result value.
        """

        last_validator = self.validators[-1]

        # if the variable 'last_validator' is an instance of
        # ProcessingValidator or CompositeValidator
        if isinstance(
                last_validator,
                (ProcessingValidator, CompositeValidator)
        ):
            return last_validator.get_result_value()

        return self.value

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """

        for validator in self.validators:

            can_continue = validator.validate()
            self.report.append_report(validator.report)

            if not can_continue:
                return False

        return True


class ProcessingValidator(Validator):
    """Special Validator for processing the value.

    Special validator for producing a result value from
    the processing of the given value.

    Attributes:
        result:
            The result produced from the initial value.
    """

    result: Any

    def __init__(self, value: Any):
        """Constructor of ProcessingValidator.

        Args:
            value:
                The value to get validated.
        """

        super().__init__(value)

        self.result = None

    @abc.abstractmethod
    def process(self) -> Any:
        """Processes the value.

        Returns the result value from processing the initial value.

        Returns:
            The result value.
        """

        raise NotImplementedError

    def get_result_value(self) -> Any:
        """Returns the result.

        Returns the result value.

        Returns:
            The result value.
        """
        return self.result

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        self.result = self.process()


class ApiMapFileValidator(CompositeValidator):
    """Validator for an API Map file.

    Validator for validating a given API Map file.
    Consists of a FileLoadValidator, a YamlLoadValidator and an ApiMapValidator.

    Attributes:
        value:
            The file name or file path as Text.
    """

    value: Text

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        load_file = LoadFileValidator(self.value)
        is_file_loaded = load_file.validate()
        self.append_report(load_file)

        # if file did not load
        if not is_file_loaded:
            return False

        load_yaml = LoadYamlValidator(load_file)
        is_yaml_loaded = load_yaml.validate()
        self.append_report(load_yaml)

        # if yaml did not load
        if not is_yaml_loaded:
            return False

        api_map = ApiMapValidator(load_yaml.get_result_value())
        can_continue = api_map.validate()
        self.append_report(api_map)

        return can_continue

    def result(self) -> bool:
        if not self.report:
            self.validate()

        return


class LoadFileValidator(ProcessingValidator):
    """Validator for loading a file.

    Validator for opening a file and reading it as Text.
    """

    def process(self) -> Any:
        """Processes the value.

        Returns the result value from processing the initial value.

        Returns:
            The result value.
        """
        with open(self.value, "r", encoding="UTF-8") as file:
            return file.read()

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        try:
            super().validate()
        except IOError as io_error:
            self.report = Report(Valid(io_error))
            return False
        else:
            self.report = Report(Valid("Load file was valid."))
            return True


class LoadYamlValidator(ProcessingValidator):
    """Validator for loading YAML.

    Validator for loading a YAML String.
    """

    def process(self) -> Dict:
        """Processes the value.

        Returns the result value from processing the initial value.

        Returns:
            The result value as a dict.
        """
        return yaml.safe_load(self.value)

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        try:
            super().validate()
        except yaml.YAMLError as yaml_error:
            self.report = Report(Valid(yaml_error))
            return False
        else:
            self.report = Report(Valid("YAML Parsing successful."))
            return True


class ApiMapValidator(CompositeValidator):
    """Validator for loading Validators.

    Validator for calling the other required Validators.
    """

    def __init__(self, value: Dict[Text, Any]):
        """Constructor of ApiMapValidator.

        Args:
            value:
                The value to get validated.
        """

        super().__init__(
            value,
            NameValidator(value),
            ApiUrlValidator(value),
            RateLimitValidator(value),
            RequestsValidator(value)
        )


class NameValidator(Validator):
    """Validator for the key 'name'.

    Validator for validating a given YAML file regarding the key 'name'.
    Checks whether 'name' exists in the file, is a String and meets the naming
    convention.

    Attributes:
        name_regex:
            The Regex pattern for valid names must meet.
            i.e. 'exchange', 'crypto_exchange'
    """

    name_regex: Text = r"^(?:[a-z.?!]+[_]{1})*[a-z.?!]+$"

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        try:
            # if the key 'name' is no part of the dict
            if "name" not in self.value:
                # throw exception
                raise KeyNotInDictError("name", self.value)
        except KeyNotInDictError as error:
            has_name = Report(Valid(error))
            self.report = has_name
        # if the key 'name' is part of the dict
        else:
            has_name = Report(Valid("Key 'name' exists."))
            name = self.value.get("name")
            try:
                # if 'name' is not a string
                if not isinstance(name, str):
                    raise WrongTypeError(str, type(name))
            except WrongTypeError as error:
                is_name_string = Report(Valid(error))
                self.report = CompositeReport(has_name, is_name_string)
            else:
                is_name_string = Report(
                    Valid("Value of keys 'name' is a String")
                )
                try:
                    # if the naming convention does not match
                    if not re.match(self.name_regex, name):
                        raise NamingConventionError(self.name_regex, name)
                except NamingConventionError as error:
                    meets_convention = Report(Valid(error))
                else:
                    meets_convention = Report(Valid("Naming convention met."))

                self.report = CompositeReport(
                    has_name,
                    is_name_string,
                    meets_convention
                )

        return True


class UrlValidator(Validator):
    """Validator for a valid URL.

    Validator for validating a given URL.
    Checks whether the URL is a String and is valid.
    """

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        try:
            # if the value of 'api_url' is not a string
            if not isinstance(self.value, str):
                raise WrongTypeError(
                    str,
                    type(self.value)
                )

        except WrongTypeError as error:
            is_url_str = Report(Valid(error))
            self.report = is_url_str
        # the value is a string
        else:
            is_url_str = Report(Valid("URL is a str."))

            try:
                url_validation_result = validators.url(self.value)
                if not url_validation_result:
                    raise UrlValidationError(self.value, url_validation_result)
            except UrlValidationError as error:
                is_url_valid = Report(Valid(error))
            # the URL is valid
            else:
                is_url_valid = Report(Valid("URL is valid."))

            self.report = CompositeReport(is_url_str, is_url_valid)

        return True


class ApiUrlValidator(Validator):
    """Validator for the key 'api_url'.

    Validator for validating a given YAML file regarding the key
    'api_url'.
    Checks whether 'api_url' exists in the root dict and is a valid URL
    according to an instance of the class UrlValidator.

    Attributes:
        value:
            The root dict that should contain the 'api_url' field.
    """

    value: Dict[Text, Any]

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        try:
            # if the key 'api_url' is not a part of the dict
            if "api_url" not in self.value:
                raise KeyNotInDictError("api_url", self.value)
        except KeyNotInDictError as error:
            has_api_url = Report(Valid(error))
            self.report = has_api_url
        else:
            # the key 'api_url' exists
            has_api_url = Report(Valid("Key 'api_url' exists."))
            api_url = self.value.get("api_url")

            # checking whether the api_url is valid
            url_validator = UrlValidator(api_url)
            url_validator.validate()
            valid_url = url_validator.report
            self.report = CompositeReport(has_api_url, valid_url)

        return True


class RateLimitValidator(Validator):
    """Validator for the field 'rate_limit'.

    Validator for validating the root or a part of the API Map
    regarding the field 'rate_limit'.
    Checks whether the optional key 'rate_limit' exists in the dict,
    the value of the key is None or is a dict.
    The fields of the dict are also checked.
    """

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        # if the key 'rate_limit' does exist
        if "rate_limit" in self.value:
            is_rate_limit_key = Report(
                Valid("Optional key 'rate_limit' exists.")
            )
            # assigns the value of the key 'rate_limit'
            rate_limit = self.value.get("rate_limit")

            # if the value of 'rate_limit' is None
            if rate_limit is None:
                has_rate_limit_value = Report(
                    Valid("Value of optional key 'rate_limit' was None.")
                )
                self.report = CompositeReport(
                    is_rate_limit_key,
                    has_rate_limit_value
                )
                return True
            # if the value of 'rate_limit' is not None
            has_rate_limit_value = Report(
                Valid("Value of optional key 'rate_limit' was not None.")
            )

            try:
                # if the value of 'rate_limit' is not a dict
                if not isinstance(rate_limit, dict):
                    raise WrongTypeError(dict, type(rate_limit))
            except WrongTypeError as error:
                is_rate_limit_dict = Report(Valid(error))
                self.report = CompositeReport(
                    is_rate_limit_key,
                    has_rate_limit_value,
                    is_rate_limit_dict
                )
            else:
                # the value of 'rate_limit' is a dict
                is_rate_limit_dict = Report(Valid("Rate limit was a dict."))

                are_keys_valid = CompositeReport()

                for key in ["max", "unit"]:
                    try:
                        # if the keys 'max'/'unit' are no part of 'rate_limit'
                        if key not in rate_limit:
                            raise KeyNotInDictError(key, rate_limit)
                    except KeyNotInDictError as error:
                        are_keys_valid.append_report(Report(Valid(error)))
                    else:
                        are_keys_valid.append_report(
                            Report(
                                Valid("Key '" + key + "' was in rate limit.")
                            )
                        )

                        value = rate_limit.get(key)

                        try:
                            # if the value of 'max' or 'unit' are no Integer
                            if not isinstance(value, int):
                                raise WrongTypeError(int, type(value))
                        except WrongTypeError as error:
                            are_keys_valid.append_report(Report(Valid(error)))
                        else:
                            are_keys_valid.append_report(
                                Report(
                                    Valid(
                                        "Value of '"
                                        + key
                                        + "' in rate limit was an int."
                                    )
                                )
                            )

                self.report = CompositeReport(
                    is_rate_limit_key,
                    has_rate_limit_value,
                    is_rate_limit_dict,
                    are_keys_valid
                )
        else:
            # the key 'rate_limit' does not exist
            is_rate_limit_key = Report(
                Valid("Optional key 'rate_limit' does not exist.")
            )

        return True


class RequestsValidator(CompositeValidator):
    """Validator for the key 'requests'.

    Validator for validating a given YAML file regarding the key
    'requests'.
    Checks whether 'requests' exists in the file and the value of the key is
    a dict.

    Attributes:
        value:
            The dict that shall be checked.
    """

    value: Dict[Text, Any]

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        try:
            # if the key 'requests' is no part of the dict
            if "requests" not in self.value:
                raise KeyNotInDictError("requests", self.value)
        except KeyNotInDictError as error:
            has_requests = Report(Valid(error))
            self.append_report(has_requests)
            return True
        else:
            # key 'requests' does exist
            has_requests = Report(Valid("Key 'requests' exists."))
            self.append_report(has_requests)
            requests = self.value.get("requests")

            try:
                # if the value of 'requests' is not a dict
                if not isinstance(requests, dict):
                    raise WrongTypeError(dict, type(requests))
            except WrongTypeError as error:
                is_requests_dict = Report(Valid(error))
                self.append_report(is_requests_dict)
                return True
            else:
                # if the value of 'requests' is a dict
                is_requests_dict = Report(
                    Valid("Value of key 'requests' is a dict.")
                )
                self.append_report(is_requests_dict)

                for api_method in requests.values():
                    self.append_validator(ApiMethodValidator(api_method))

                return super().validate()


class ApiMethodValidator(CompositeValidator):
    """Validator for loading Validators.

    Validator for calling the other required Validators.
    """

    def __init__(self, value):
        """Constructor of ApiMethodValidator.

        Args:
            value:
                The value to get validated.
        """

        super().__init__(
            value,
            RequestValidator(value),
            ResponseValidator(value),
            MappingValidator(value)
        )


class RequestValidator(CompositeValidator):
    """Validator for the key 'request'.

    Validator for validating a given API method dict regarding the key
    'request'.
    Checks whether 'request' exists in the dict and the value of the key is
    a dict.
    Also delegation of validation of dict fields to other Validators.

    Attributes:
        value:
            The dict that shall be checked.
    """

    value: Dict[Text, Any]

    def __init__(self, value):
        """Constructor of RequestValidator.

        Args:
            value:
                The value to get validated.
        """

        request: Dict[Text, Any] = value.get("request")
        super().__init__(
            value,
            # TODO: Validate whether all keys of 'template' are in 'pair_template' or 'params'
            TemplateValidator(request),
            PairTemplateValidator(request),
            ParamsValidator(request)
        )

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        try:
            # if the key 'request' does not exist
            if "request" not in self.value:
                raise KeyNotInDictError("request", self.value)
        except KeyNotInDictError as error:
            has_request = Report(Valid(error))
            self.append_report(has_request)
            return True
        else:
            # 'request' does exist
            has_request = Report(Valid("Key 'request' exists."))
            self.append_report(has_request)
            request = self.value.get("request")

            try:
                # if the value of 'request' is not a dict
                if not isinstance(request, dict):
                    raise WrongTypeError(dict, type(request))
            except WrongTypeError as error:
                is_request_dict = Report(Valid(error))
                self.append_report(is_request_dict)
                return True
            else:
                # the value of 'request' is a dict
                is_request_dict = Report(Valid("Value of key 'request' "
                                               "is a dict."))
                self.append_report(is_request_dict)

                return super().validate()


class TemplateValidator(Validator):
    """Validator for the key 'template'.

    Validator for validating a given 'request' dict regarding the key
    'template'.
    Checks whether 'template' exists in the 'request' and the value of the key
    is a String.

    Attributes:
        value:
            The dict that shall be checked.
    """

    value: Dict[Text, Any]

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        try:
            # if the key 'template' does not exist
            if "template" not in self.value:
                raise KeyNotInDictError("template", self.value)
        except KeyNotInDictError as error:
            has_template = Report(Valid(error))
            self.report = has_template
        else:
            # 'template' does exist
            has_template = Report(Valid("Key 'template' exists."))
            template = self.value.get("template")

            try:
                # if the value of 'template' is not a string
                if not isinstance(template, str):
                    raise WrongTypeError(str, type(self.value))
            except WrongTypeError as error:
                is_template_str = Report(Valid(error))
                self.report = is_template_str
            else:
                # the value of 'template' is a string
                is_template_str = Report(Valid("template is a str."))

            self.report = CompositeReport(has_template, is_template_str)

        # TODO: Check the cases: 'template: ""' and 'template: "{currency_pair}/products"'

        return True


class PairTemplateValidator(Validator):
    """Validator for the key 'pair_template'.

    Validator for validating a given 'request' dict regarding the key
    'pair_template'.
    Checks whether the optional key 'pair_template' exists in the file, the
    value of the key is None or it is a dict.

    Attributes:
        value:
            The dict that shall be checked.
    """

    value: Dict[Text, Any]

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """

        # if the key 'pair_template' does exist
        if "pair_template" in self.value:
            has_pair_template_key = Report(
                Valid("Optional key 'pair_template' exists.")
            )

            pair_template = self.value.get("pair_template")

            # if the value of 'pair_template' is None
            if pair_template is None:
                has_pair_template_value = Report(
                    Valid("Value of optional key 'pair_template' was None.")
                )
                self.report = CompositeReport(
                    has_pair_template_key,
                    has_pair_template_value
                )
                return True

            # if the value of 'pair_template' is not None
            has_pair_template_value = Report(
                Valid("Value of optional key 'pair_template' was not None.")
            )

            try:
                # if the value of 'pair_template' is not a dict
                if not isinstance(pair_template, dict):
                    raise WrongTypeError(dict, type(pair_template))
            except WrongTypeError as error:
                is_pair_template_dict = Report(Valid(error))
            else:
                is_pair_template_dict = Report(Valid("pair_template was "
                                                     "a dict."))

            self.report = CompositeReport(
                has_pair_template_key,
                has_pair_template_value,
                is_pair_template_dict
            )

            try:
                # if the value of 'pair_template' is not the key 'template'
                if "template" not in pair_template:
                    raise KeyNotInDictError("template", pair_template)
            except KeyNotInDictError as error:
                has_template_key = Report(Valid(error))
                self.report.append_report(has_template_key)
            else:
                # if the key 'template' does exist
                has_template_key = Report(Valid("Key 'template' exists."))
                template_value = pair_template.get("template")

                try:
                    # if the value of 'template' is not a string
                    if not isinstance(template_value, str):
                        raise WrongTypeError(str, type(template_value))
                except WrongTypeError as error:
                    is_template_str = Report(Valid(error))
                    self.report.append_report(
                        CompositeReport(
                            has_template_key,
                            is_template_str
                        )
                    )
                else:
                    is_template_str = Report(
                        Valid("Value of key 'template' was a str.")
                    )

                    substring_reports = CompositeReport()

                    for substring in ("{first}", "{second}"):
                        try:
                            # if the value of 'template' is neither
                            # "{first}" nor "{second}"
                            if substring not in template_value:
                                raise SubstringNotInStringError(
                                    substring,
                                    template_value
                                )
                        except SubstringNotInStringError as error:
                            substring_report = Report(Valid(error))
                        else:
                            substring_report = Report(
                                Valid(
                                    "Substring "
                                    + substring
                                    + " was in template"
                                )
                            )

                        substring_reports.append_report(substring_report)

                    self.report.append_report(
                        CompositeReport(
                            has_template_key,
                            is_template_str,
                            substring_reports
                        )
                    )
            # if the key 'lower_case' does exist
            if "lower_case" in pair_template:
                has_lower_case_key = Report(
                    Valid("Optional key 'lower_case' exists.")
                )
                lower_case_value = pair_template.get("lower_case")

                try:
                    # if the value of 'lower_case' is not a boolean
                    if not isinstance(lower_case_value, bool):
                        raise WrongTypeError(bool, type(lower_case_value))
                except WrongTypeError as error:
                    is_lower_case_value_bool = Report(Valid(error))
                else:
                    is_lower_case_value_bool = Report(
                        Valid("Value of key 'lower_case' is a bool.")
                    )

                self.report.append_report(
                    CompositeReport(
                        has_lower_case_key,
                        is_lower_case_value_bool
                    )
                )

            else:
                # if the key 'lower_case' does not exist
                has_alias_key = Report(
                    Valid("Optional key 'alias' does not exist.")
                )
                self.report.append_report(has_alias_key)

            # if the key 'alias' does exist
            if "alias" in pair_template:
                has_alias_key = Report(
                    Valid("Optional key 'alias' exists.")
                )
                alias_value = pair_template.get("alias")

                try:
                    # if the value of 'alias' is not a string
                    if not isinstance(alias_value, str):
                        raise WrongTypeError(str, type(alias_value))
                except WrongTypeError as error:
                    is_alias_value_str = Report(Valid(error))
                else:
                    is_alias_value_str = Report(
                        Valid("Value of key 'alias' is a str.")
                    )

                self.report.append_report(
                    CompositeReport(
                        has_alias_key,
                        is_alias_value_str
                    )
                )

            else:
                # if the key 'alias' does not exist
                has_alias_key = Report(
                    Valid("Optional key 'alias' does not exist.")
                )
                self.report.append_report(has_alias_key)

        else:
            # if the key 'pair_template' does not exist
            is_pair_template_key = Report(
                Valid("Optional key 'pair_template' does not exist.")
            )
            self.report = is_pair_template_key

        return True


class ParamsValidator(CompositeValidator):
    """Validator for the key 'params'.

    Validator for validating a given 'request' dict regarding the key
    'params'.
    Checks whether the optional key 'params' exists in the file, the
    value of the key is None or it is a dict.

    Attributes:
        value:
            The dict that shall be checked.
    """

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """

        # if the key 'params' does exist
        if "params" in self.value:
            has_params_key = Report(
                Valid("Optional key 'params' does exist.")
            )
            self.append_report(has_params_key)
            params_value = self.value.get("params")

            # if the value of 'params' is None
            if params_value is None:
                is_params_value_none = Report(
                    Valid("Value of optional key 'params' is None")
                )
                self.append_report(is_params_value_none)

                return True

            try:
                # if the value of 'params' is not a dict
                if not isinstance(params_value, dict):
                    raise WrongTypeError(dict, type(params_value))
            except WrongTypeError as error:
                is_params_value_dict = Report(Valid(error))
                self.append_report(is_params_value_dict)
            else:
                # if the value of 'params' is a dict
                is_params_value_dict = Report(
                    Valid("Value of optional key 'params' is a dict.")
                )
                self.append_report(is_params_value_dict)

                for param in params_value.values():
                    self.append_validator(ParamValidator(param))

                return super().validate()

        else:
            # if the key 'params' does not exist
            has_params_key = Report(
                Valid("Optional key 'params' does not exist.")
            )
            self.report = has_params_key

        return True


class ParamValidator(Validator):
    """Validator for a certain parameter.

    Validator for validating a given 'params' dict regarding a certain
    parameter.
    Checks whether the value of param is a dict and the key allowed or default
    is in the file.
    """

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """

        try:
            # if the key 'param' is not a dict
            if not isinstance(self.value, dict):
                raise WrongTypeError(dict, type(self.value))
        except WrongTypeError as error:
            is_value_dict = Report(Valid(error))
            self.report = is_value_dict
            return True
        else:
            # if the key 'param' is a dict
            is_value_dict = Report(Valid("Value of param is a dict."))
            self.report = CompositeReport(is_value_dict)

            # if the key 'allowed' does exist
            if "allowed" in self.value:
                has_allowed_key = Report(
                    Valid("Optional key 'allowed' is in dict.")
                )
                allowed_value = self.value.get("allowed")

                try:
                    # if the value of 'allowed' is not a list
                    if not isinstance(allowed_value, list):
                        raise WrongTypeError(list, type(allowed_value))
                except WrongTypeError as error:
                    is_allowed_value_list = Report(Valid(error))
                    self.report.append_report(
                        CompositeReport(
                            has_allowed_key,
                            is_allowed_value_list
                        )
                    )
                else:
                    # if the value of 'allowed' is a list
                    is_allowed_value_list = Report(
                        Valid("Value of key 'allowed' is a list.")
                    )

                    try:
                        # if the list 'allowed' is empty
                        if not allowed_value:
                            raise ValueError(
                                "Value of key 'allowed' is an empty list."
                            )
                    except ValueError as error:
                        is_allowed_list_empty = Report(Valid(error))
                    else:
                        is_allowed_list_empty = Report(
                            Valid("Value of key 'allowed' "
                                  "is a non-empty list.")
                        )

                    self.report.append_report(
                        CompositeReport(
                            has_allowed_key,
                            is_allowed_value_list,
                            is_allowed_list_empty
                        )
                    )

            else:
                # the key 'allowed' is not a dict
                has_allowed_key = Report(
                    Valid("Optional key 'allowed' is not in dict.")
                )
                self.report.append_report(has_allowed_key)

            # if the key 'default' exists
            if "default" in self.value:
                has_default_key = Report(
                    Valid("Optional key 'default' is in dict.")
                )
                default_value = self.value.get("default")

                try:
                    # if the value of 'default' is None
                    if default_value is None:
                        raise ValueError("Value of key 'default' is None.")
                except ValueError as error:
                    is_default_value_none = Report(Valid(error))
                else:
                    # if the value of 'default' is not None
                    is_default_value_none = Report(
                        Valid("Value of key 'default' is not None.")
                    )
                self.report.append_report(
                    CompositeReport(
                        has_default_key,
                        is_default_value_none
                    )
                )

            else:
                # if the key 'default' does not exist
                has_default_key = Report(
                    Valid("Optional key 'default' is not in dict.")
                )
                self.report.append_report(has_default_key)

        return True


class ResponseValidator(ProcessingValidator):
    """Validator for the key 'response'.

    Validator for validating a given API method dict regarding the key
    'response'.
    Checks whether 'response' exists in the dict and the value of the key is
    a dict.

    Attributes:
        value:
            The dict that shall be checked.
    """

    value: Dict[Text, Any]

    def process(self) -> Any:
        """Processes the value.

        Returns the result value from processing the initial value.

        Returns:
            The result value as a dict.
        """
        # TODO: implement

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        try:
            # if the key 'response' does not exist
            if "response" not in self.value:
                raise KeyNotInDictError("response", self.value)
        except KeyNotInDictError as error:
            has_response = Report(Valid(error))
            self.report = has_response
        else:
            # if the key 'response' does exist
            has_response = Report(Valid("Key 'response' exists."))
            response = self.value.get("response")

            try:
                # if the value of 'response' is a dict
                if not isinstance(response, dict):
                    raise WrongTypeError(dict, type(response))
            except WrongTypeError as error:
                is_response_dict = Report(Valid(error))
                self.report = is_response_dict
            else:
                # if the value of 'response' is a dict
                is_response_dict = Report(Valid("Value of key 'response' "
                                                "is a dict."))

                self.report = CompositeReport(has_response, is_response_dict)

        return True


class MappingValidator(CompositeValidator):
    """Validator for the key 'mapping'.

    Validator for validating a given API method dict regarding the key
    'mapping'.
    Checks whether 'mapping' exists in the dict and the value of the key is
    a list.

    Attributes:
        value:
            The dict that shall be checked.
    """

    value: Dict[Text, Any]

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """
        try:
            # if the key 'mapping' does not exist
            if "mapping" not in self.value:
                raise KeyNotInDictError("mapping", self.value)
        except KeyNotInDictError as error:
            has_mapping_key = Report(Valid(error))
            self.append_report(has_mapping_key)
            return True
        else:
            # if the key 'mapping' does exist
            has_mapping_key = Report(
                Valid("Key 'mapping' exists.")
            )
            self.append_report(has_mapping_key)

            mapping = self.value.get("mapping")

            try:
                # if the value of 'mapping' is not a list
                if not isinstance(mapping, list):
                    raise WrongTypeError(list, type(mapping))
            except WrongTypeError as error:
                is_mapping_list = Report(Valid(error))
                self.append_report(is_mapping_list)
                return True
            else:
                # if the value of 'mapping' is a list
                is_mapping_list = Report(
                    Valid("Value of key 'mapping' was a list.")
                )
                self.append_report(is_mapping_list)

                for mapping_entry in mapping:
                    self.append_validator(MappingEntryValidator(mapping_entry))

                return super().validate()


class MappingEntryValidator(CompositeValidator):
    """Validator for an entry of the key 'mapping'.

    Validator for validating a given API method dict regarding an entry
    of the key 'mapping'.

    Attributes:
        value:
            The dict that shall be checked.
    """

    value: Dict[Text, Any]

    def __init__(self, value):
        """Constructor of MappingEntryValidator.

        Args:
            value:
                The value to get validated.
        """

        super().__init__(
            value,
            KeyValidator(value),
            PathValidator(value),
            TypeValidator(value)
        )

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """

        try:
            # if the mapping entry is not a dict
            if not isinstance(self.value, dict):
                raise WrongTypeError(dict, type(self.value))
        except WrongTypeError as error:
            is_mapping_dict = Report(Valid(error))
            self.append_report(is_mapping_dict)
            return True
        else:
            # if the mapping entry is a dict
            is_mapping_dict = Report(
                Valid("Mapping entry was a dict.")
            )
            self.append_report(is_mapping_dict)

            return super().validate()


class KeyValidator(Validator):
    """Validator for the key 'key'.

    Validator for validating a given 'map' dict regarding the key
    'key'.
    Checks whether 'key' exists in the 'map' and the value of the key is
    a String.

    Attributes:
        value:
            The dict that shall be checked.
    """

    value: Dict[Text, Any]

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """

        mapping = self.value

        try:
            # if the key 'key' is no part of the dict 'mapping'
            if "key" not in mapping:
                raise KeyNotInDictError("key", mapping)
        except KeyNotInDictError as error:
            has_key_key = Report(Valid(error))
            self.report = has_key_key
        else:
            # 'key' does exist
            has_key_key = Report(Valid("Key 'key' exists."))

            key_value = mapping.get("key")

            try:
                # if the value of 'key' is not a string
                if not isinstance(key_value, str):
                    raise WrongTypeError(str, type(key_value))
            except WrongTypeError as error:
                is_key_str = Report(Valid(error))
                self.report = is_key_str
            else:
                # if the value of 'key' is a string
                is_key_str = Report(Valid("Value of key 'key' was a str."))

                self.report = CompositeReport(has_key_key, is_key_str)
                # TODO: Compare values of keys with database_column_mapping
#!!!
        return True


class PathValidator(Validator):
    """Validator for the key 'path'.

    Validator for validating a given 'map' dict regarding the key
    'path'.
    Checks whether 'path' exists in the 'map' and the value of the key is
    a list.

    Attributes:
        value:
            The dict that shall be checked.
    """

    value: Dict[Text, Any]

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """

        mapping = self.value

        try:
            # if the key 'path' does not exist in 'mapping'
            if "path" not in mapping:
                raise KeyNotInDictError("path", mapping)
        except KeyNotInDictError as error:
            has_path_key = Report(Valid(error))
            self.report = has_path_key
        else:
            # if the key 'path' does exist in 'mapping'
            has_path_key = Report(Valid("Key 'path' exists."))

            path_value = mapping.get("path")

            try:
                # if the value of 'path' is not a list
                if not isinstance(path_value, list):
                    raise WrongTypeError(list, type(path_value))
            except WrongTypeError as error:
                is_path_list = Report(Valid(error))
                self.report = is_path_list
            else:
                # if the value of 'path' is a list
                is_path_list = Report(Valid("Value of key 'path' was a list."))

                self.report = CompositeReport(has_path_key, is_path_list)

        return True


class TypeValidator(Validator):
    """Validator for the key 'type'.

    Validator for validating a given 'map' dict regarding the key
    'type'.
    Checks whether 'type' exists in the 'map' and the value of the key is
    a list.

    Attributes:
        value:
            The dict that shall be checked.
    """

    value: Dict[Text, Any]

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        Returns:
            Whether further Validators may continue validating.
        """

        mapping = self.value

        try:
            # if the key 'type' does not exist in 'mapping'
            if "type" not in mapping:
                raise KeyNotInDictError("type", mapping)
        except KeyNotInDictError as error:
            has_type_key = Report(Valid(error))
            self.report = has_type_key
        else:
            # if the key 'type' does exist in 'mapping'
            has_type_key = Report(Valid("Key 'type' exists."))

            type_value = mapping.get("type")

            try:
                # if the value of 'type' is not a list
                if not isinstance(type_value, list):
                    raise WrongTypeError(list, type(type_value))
            except WrongTypeError as error:
                is_type_list = Report(Valid(error))
                self.report = is_type_list
            else:
                # if the value of 'type' is a list
                is_type_list = Report(
                    Valid("Value of key 'type' was a list."))

                self.report = CompositeReport(has_type_key, is_type_list)

        return True

#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
        -RequestMappingValidator

Authors:
    Carolina Keil
    Martin Schorfmann
    Paul Böttger

Since:
    19.03.2019

Version:
    26.04.2019
"""
from typing import Any, Text, Dict

import oyaml as yaml
import validators

# pylint: disable=too-many-lines
from model.database.tables import ExchangeCurrencyPair, Ticker, HistoricRate, OrderBook, Trade
from model.validating.base import Report, CompositeReport, Valid, Validator, CompositeValidator, ProcessingValidator
from model.validating.errors import KeyNotInDictError, SubstringNotInStringError, WrongTypeError, UrlValidationError, \
    NamingConventionError


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
        """
        TODO: Fill out
        """
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

    def process(self) -> dict:
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
            RequestsValidator(value),
            RequestMappingValidator(value['requests'])
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
                    pass  # TODO: Philipp: Naming conventions fix
                    # if the naming convention does not match
                    # if not re.match(self.name_regex, name):
                    #    raise NamingConventionError(self.name_regex, name)
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
                if self.value != "" and not url_validation_result:
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
            # TODO: Philipp: Correct just add?
            self.report = CompositeReport(is_rate_limit_key)

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

    def __init__(self, value: Any):
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

    def __init__(self, value: Any):
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
                    # TODO: Philipp: is None for alias ok?
                    if alias_value is not None and not isinstance(alias_value, str):
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
                    if not isinstance(allowed_value, dict):
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
                        Valid("Value of key 'allowed' is a dict.")
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
                                  "is a non-empty dict.")
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

    def __init__(self, value: Any):
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


class RequestMappingValidator(Validator):
    """
    TODO: Fill out
    """
    value: Dict[Text, Any]

    def determine_table(self, table_name: str) -> dict:
        """
        Returns the method that is to execute based on the given request name.

        @param table_name: str
            Name of the request.
        @return:
            Method for the request name or a string that the request is false.
        """
        possible_class = {
            "currency_pairs":
                {'table': ExchangeCurrencyPair},
            "tickers":
                {'table': Ticker},
            "historic_rates":
                {'table': HistoricRate},
            "order_books":
                {'table': OrderBook},
            "trades":
                {'table': Trade}
        }
        return possible_class.get(table_name, lambda: "Invalid request class.")

    def determine_primary_keys(self, table_name: str) -> list:
        """
        TODO: Fill out
        """
        possible_primary_keys = {
            "currency_pairs":
                [],
            "tickers":
                ['time'],
            "historic_rates":
                ['time'],
            "order_books":
                ['position'],
            "trades":
                ['time'],
            "ohlcvm":
                ['time']
        }
        return possible_primary_keys.get(table_name, lambda: "Invalid request class")

    def validate(self) -> bool:
        # TODO: Philipp: Does not work, fix later
        self.report = Report(Valid("Need to be fixed later."))
        return True
        # requests = self.value
        # for request in requests.keys():
        #     table = self.determine_table(request)
        #
        #     if not isinstance(table, dict):
        #         return False
        #
        #     match_table = table['table']
        #     class_keys = [key.name for key in inspect(match_table).columns]
        #     for mapping in requests[request]['mapping']:
        #         mapping_key = mapping['key']
        #         if mapping_key != 'currency_pair_first' or mapping_key != 'currency_pair_second':
        #             try:
        #                 class_keys.index(mapping_key)
        #             except ValueError as error:
        #                 is_type_list = Report(Valid(error))
        #                 self.report = is_type_list
        #     primary_keys = self.determine_primary_keys(request)
        #     for primary_key in primary_keys:
        #         if primary_key == requests[request]['mapping']:
        #             return False
        # return True

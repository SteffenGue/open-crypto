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
import re
from typing import Any, Text, Dict

import oyaml as yaml
import validators

from model.database.tables import ExchangeCurrencyPair, Ticker, HistoricRate, OrderBook, Trade
from model.validating.base import Report, CompositeReport, Validator, CompositeValidator, ProcessingValidator
from model.validating.errors import KeyNotInDictError, SubstringNotInStringError, WrongTypeError, UrlValidationError, \
    NamingConventionError


# pylint: disable=too-many-lines


class ApiMapFileValidator(CompositeValidator):
    """
    Validator for validating a given API Map file.

    Consists of a FileLoadValidator, a YamlLoadValidator and an ApiMapValidator.

    Attributes:
        value:
            The file name or file path as Text.
    """

    value: Text

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        load_file = LoadFileValidator(self.value)
        is_file_loaded = load_file.validate()
        self.append_report(load_file)

        if not is_file_loaded:
            return False

        load_yaml = LoadYamlValidator(load_file)
        is_yaml_loaded = load_yaml.validate()
        self.append_report(load_yaml)

        if not is_yaml_loaded:
            return False

        api_map = ApiMapValidator(load_yaml.get_result_value())
        can_continue = api_map.validate()
        self.append_report(api_map)

        return can_continue


class LoadFileValidator(ProcessingValidator):
    """
    Validator for opening a file and reading it as Text.
    """

    def process(self) -> Any:
        """
        Processes the value.

        @return: The result value from processing the initial value.
        """
        with open(self.value, "r", encoding="UTF-8") as file:
            return file.read()

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            super().validate()
        except IOError as io_error:
            self.report = Report(io_error)
            return False
        else:
            self.report = Report("Load file was valid.")
            return True


class LoadYamlValidator(ProcessingValidator):
    """
    Validator for loading a YAML String.
    """

    def process(self) -> dict:
        """
        Processes the value.

        @return: The result value from processing the initial value.
        """
        return yaml.safe_load(self.value)

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            super().validate()
        except yaml.YAMLError as yaml_error:
            self.report = Report(yaml_error)
            return False
        else:
            self.report = Report("YAML Parsing successful.")
            return True


class ApiMapValidator(CompositeValidator):
    """
    Validator for calling the other required Validators.
    """

    def __init__(self, value: Dict[Text, Any]):
        """
        Constructor of ApiMapValidator.

        @param value: The value to get validated.
        """
        super().__init__(
            value,
            NameValidator(value),
            ApiUrlValidator(value),
            RateLimitValidator(value),
            RequestsValidator(value),
            RequestMappingValidator(value["requests"])
        )


class NameValidator(Validator):
    """
    Validator to validate a given exchange with respect to the 'name' key.

    Checks if 'name' exists in the file, is a string and conforms to the naming convention.

    Attributes:
        name_regex:
            The Regex pattern for valid names must meet.
            i.e. 'exchange', 'crypto_exchange'
    """
    name_pattern: Text = r"[a-z0-9\-_]+(\.){0,1}[a-z0-9]+"

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "name" not in self.value:
                raise KeyNotInDictError("name", self.value)
        except KeyNotInDictError as error:
            has_name = Report(error)
            self.report = has_name
        else:
            has_name = Report("Key 'name' exists.")
            name = self.value.get("name")
            try:
                if not isinstance(name, str):
                    raise WrongTypeError(str, type(name))
            except WrongTypeError as error:
                is_name_string = Report(error)
                self.report = CompositeReport(has_name, is_name_string)
            else:
                is_name_string = Report("Value of the 'name' key is a string.")
                try:
                    if not re.fullmatch(self.name_pattern, name):
                        raise NamingConventionError(self.name_pattern, name)
                except NamingConventionError as error:
                    meets_convention = Report(error)
                else:
                    meets_convention = Report("Naming convention met.")

                self.report = CompositeReport(has_name, is_name_string, meets_convention)

        return True


class UrlValidator(Validator):
    """
    Validator for validating a given URL.

    Checks whether the URL is a String and is valid.
    """

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            if not isinstance(self.value, str):
                raise WrongTypeError(str, type(self.value))
        except WrongTypeError as error:
            is_url_str = Report(error)
            self.report = is_url_str
        else:
            is_url_str = Report("URL is a str.")
            try:
                url_validation_result = validators.url(self.value)
                if self.value != "" and not url_validation_result:
                    raise UrlValidationError(self.value, url_validation_result)
            except UrlValidationError as error:
                is_url_valid = Report(error)
            else:
                is_url_valid = Report("URL is valid.")

            self.report = CompositeReport(is_url_str, is_url_valid)

        return True


class ApiUrlValidator(Validator):
    """
    Validator for validating a given YAML file regarding the key 'api_url'.

    Checks whether 'api_url' exists in the root dict and is a valid URL
    according to an instance of the class UrlValidator.

    Attributes:
        value:
            The root dict that should contain the 'api_url' field.
    """
    value: Dict[Text, Any]

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "api_url" not in self.value:
                raise KeyNotInDictError("api_url", self.value)
        except KeyNotInDictError as error:
            has_api_url = Report(error)
            self.report = has_api_url
        else:
            has_api_url = Report("Key 'api_url' exists.")
            api_url = self.value.get("api_url")

            url_validator = UrlValidator(api_url)
            url_validator.validate()

            self.report = CompositeReport(has_api_url, url_validator.report)

        return True


class RateLimitValidator(Validator):
    """
    Validator for the field 'rate_limit'.

    Validator for validating the root or a part of the API Map
    regarding the field 'rate_limit'.
    Checks whether the optional key 'rate_limit' exists in the dict,
    the value of the key is None or is a dict.
    The fields of the dict are also checked.
    """

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        if "rate_limit" in self.value:
            is_rate_limit_key = Report("Optional key 'rate_limit' exists.")
            rate_limit = self.value.get("rate_limit")

            if rate_limit is None:
                has_rate_limit_value = Report("Value of optional key 'rate_limit' was None.")
                self.report = CompositeReport(
                    is_rate_limit_key,
                    has_rate_limit_value
                )
                return True
            has_rate_limit_value = Report("Value of optional key 'rate_limit' was not None.")

            try:
                if not isinstance(rate_limit, dict):
                    raise WrongTypeError(dict, type(rate_limit))
            except WrongTypeError as error:
                is_rate_limit_dict = Report(error)
                self.report = CompositeReport(
                    is_rate_limit_key,
                    has_rate_limit_value,
                    is_rate_limit_dict
                )
            else:
                is_rate_limit_dict = Report("Rate limit was a dict.")
                are_keys_valid = CompositeReport()

                for key in ["max", "unit"]:
                    try:
                        if key not in rate_limit:
                            raise KeyNotInDictError(key, rate_limit)
                    except KeyNotInDictError as error:
                        are_keys_valid.append_report(Report(error))
                    else:
                        are_keys_valid.append_report(Report("Key '" + key + "' was in rate limit."))

                        value = rate_limit.get(key)

                        try:
                            if not isinstance(value, int):
                                raise WrongTypeError(int, type(value))
                        except WrongTypeError as error:
                            are_keys_valid.append_report(Report(error))
                        else:
                            are_keys_valid.append_report(Report(f"Value of '{key}' in rate limit was an int."))

                self.report = CompositeReport(
                    is_rate_limit_key,
                    has_rate_limit_value,
                    is_rate_limit_dict,
                    are_keys_valid
                )
        else:
            self.report = CompositeReport(Report("Optional key 'rate_limit' does not exist."))

        return True


class RequestsValidator(CompositeValidator):
    """
    Validator for validating a given YAML file regarding the key 'requests'.

    Checks whether 'requests' exists in the file and the value of the key is a dict.

    Attributes:
        value:
            The dict that shall be checked.
    """
    value: Dict[Text, Any]

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "requests" not in self.value:
                raise KeyNotInDictError("requests", self.value)
        except KeyNotInDictError as error:
            has_requests = Report(error)
            self.append_report(has_requests)
            return True
        else:
            has_requests = Report("Key 'requests' exists.")
            self.append_report(has_requests)
            requests = self.value.get("requests")

            try:
                if not isinstance(requests, dict):
                    raise WrongTypeError(dict, type(requests))
            except WrongTypeError as error:
                is_requests_dict = Report(error)
                self.append_report(is_requests_dict)
                return True
            else:
                is_requests_dict = Report("Value of key 'requests' is a dict.")
                self.append_report(is_requests_dict)

                for api_method in requests.values():
                    self.append_validator(ApiMethodValidator(api_method))
                return super().validate()


class ApiMethodValidator(CompositeValidator):
    """
    Validator for calling the other required Validators.
    """

    def __init__(self, value: Any):
        """
        Constructor of ApiMethodValidator.

        @param value: The value to get validated.
        """
        super().__init__(
            value,
            RequestValidator(value),
            ResponseValidator(value),
            MappingValidator(value)
        )


class RequestValidator(CompositeValidator):
    """
    Validator for validating a given API method dict regarding the key 'request'.

    Checks whether 'request' exists in the dict and the value of the key is a dict.
    Also delegation of validation of dict fields to other Validators.

    Attributes:
        value:
            The dict that shall be checked.
    """
    value: Dict[Text, Any]

    def __init__(self, value: Any):
        """
        Constructor of RequestValidator.

        @param value: The value to get validated.
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
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "request" not in self.value:
                raise KeyNotInDictError("request", self.value)
        except KeyNotInDictError as error:
            has_request = Report(error)
            self.append_report(has_request)
            return True
        else:
            has_request = Report("Key 'request' exists.")
            self.append_report(has_request)
            request = self.value.get("request")

            try:
                if not isinstance(request, dict):
                    raise WrongTypeError(dict, type(request))
            except WrongTypeError as error:
                self.append_report(Report(error))
                return True
            else:
                self.append_report(Report("Value of key 'request' is a dict."))
                return super().validate()


class TemplateValidator(Validator):
    """
    Validator for validating a given 'request' dict regarding the key 'template'.

    Checks whether 'template' exists in the 'request' and the value of the key is a String.

    Attributes:
        value:
            The dict that shall be checked.
    """
    value: Dict[Text, Any]

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "template" not in self.value:
                raise KeyNotInDictError("template", self.value)
        except KeyNotInDictError as error:
            has_template = Report(error)
            self.report = has_template
        else:
            has_template = Report("Key 'template' exists.")
            template = self.value.get("template")

            try:
                if not isinstance(template, str):
                    raise WrongTypeError(str, type(self.value))
            except WrongTypeError as error:
                is_template_str = Report(error)
                self.report = is_template_str
            else:
                is_template_str = Report("template is a str.")

            self.report = CompositeReport(has_template, is_template_str)

        # TODO: Check the cases: 'template: ""' and 'template: "{currency_pair}/products"'

        return True


class PairTemplateValidator(Validator):
    """
    Validator for validating a given 'request' dict regarding the key 'pair_template'.

    Checks whether the optional key 'pair_template' exists in the file, the value of the key is None or it is a dict.

    Attributes:
        value:
            The dict that shall be checked.
    """
    value: Dict[Text, Any]

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        if "pair_template" in self.value:
            has_pair_template_key = Report("Optional key 'pair_template' exists.")

            pair_template = self.value.get("pair_template")

            if pair_template is None:
                has_pair_template_value = Report("Value of optional key 'pair_template' was None.")
                self.report = CompositeReport(
                    has_pair_template_key,
                    has_pair_template_value
                )
                return True

            has_pair_template_value = Report("Value of optional key 'pair_template' was not None.")

            try:
                if not isinstance(pair_template, dict):
                    raise WrongTypeError(dict, type(pair_template))
            except WrongTypeError as error:
                is_pair_template_dict = Report(error)
            else:
                is_pair_template_dict = Report("pair_template was a dict.")

            self.report = CompositeReport(
                has_pair_template_key,
                has_pair_template_value,
                is_pair_template_dict
            )

            try:
                if "template" not in pair_template:
                    raise KeyNotInDictError("template", pair_template)
            except KeyNotInDictError as error:
                has_template_key = Report(error)
                self.report.append_report(has_template_key)
            else:
                has_template_key = Report("Key 'template' exists.")
                template_value = pair_template.get("template")

                try:
                    if not isinstance(template_value, str):
                        raise WrongTypeError(str, type(template_value))
                except WrongTypeError as error:
                    is_template_str = Report(error)
                    self.report.append_report(
                        CompositeReport(
                            has_template_key,
                            is_template_str
                        )
                    )
                else:
                    is_template_str = Report("Value of key 'template' was a str.")

                    substring_reports = CompositeReport()

                    for substring in ("{first}", "{second}"):
                        try:
                            if substring not in template_value:
                                raise SubstringNotInStringError(substring, template_value)
                        except SubstringNotInStringError as error:
                            if substring_reports:
                                substring_report = Report(f"Optional substring {substring} was not in template")
                            else:
                                substring_report = Report(error)
                        else:
                            substring_report = Report(f"Substring {substring} was in template")

                        substring_reports.append_report(substring_report)

                    self.report.append_report(
                        CompositeReport(
                            has_template_key,
                            is_template_str,
                            substring_reports
                        )
                    )
            if "lower_case" in pair_template:
                has_lower_case_key = Report("Optional key 'lower_case' exists.")
                lower_case_value = pair_template.get("lower_case")

                try:
                    if not isinstance(lower_case_value, bool):
                        raise WrongTypeError(bool, type(lower_case_value))
                except WrongTypeError as error:
                    is_lower_case_value_bool = Report(error)
                else:
                    is_lower_case_value_bool = Report("Value of key 'lower_case' is a bool.")

                self.report.append_report(
                    CompositeReport(
                        has_lower_case_key,
                        is_lower_case_value_bool
                    )
                )

            else:
                has_alias_key = Report("Optional key 'alias' does not exist.")
                self.report.append_report(has_alias_key)

            if "alias" in pair_template:
                has_alias_key = Report("Optional key 'alias' exists.")
                alias_value = pair_template.get("alias")

                try:
                    if alias_value is not None and not isinstance(alias_value, str):
                        raise WrongTypeError(str, type(alias_value))
                except WrongTypeError as error:
                    is_alias_value_str = Report(error)
                else:
                    is_alias_value_str = Report("Value of key 'alias' is a str.")

                self.report.append_report(
                    CompositeReport(
                        has_alias_key,
                        is_alias_value_str
                    )
                )

            else:
                has_alias_key = Report("Optional key 'alias' does not exist.")
                self.report.append_report(has_alias_key)

        else:
            is_pair_template_key = Report("Optional key 'pair_template' does not exist.")
            self.report = is_pair_template_key

        return True


class ParamsValidator(CompositeValidator):
    """
    Validator for validating a given 'request' dict regarding the key 'params'.

    Checks whether the optional key 'params' exists in the file, the value of the key is None or it is a dict.

    Attributes:
        value:
            The dict that shall be checked.
    """

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        if "params" in self.value:
            has_params_key = Report("Optional key 'params' does exist.")
            self.append_report(has_params_key)
            params_value = self.value.get("params")

            if params_value is None:
                is_params_value_none = Report("Value of optional key 'params' is None")
                self.append_report(is_params_value_none)

                return True

            try:
                if not isinstance(params_value, dict):
                    raise WrongTypeError(dict, type(params_value))
            except WrongTypeError as error:
                is_params_value_dict = Report(error)
                self.append_report(is_params_value_dict)
            else:
                is_params_value_dict = Report("Value of optional key 'params' is a dict.")
                self.append_report(is_params_value_dict)

                for param in params_value.values():
                    self.append_validator(ParamValidator(param))

                return super().validate()

        else:
            has_params_key = Report("Optional key 'params' does not exist.")
            self.report = has_params_key

        return True


class ParamValidator(Validator):
    """
    Validator for validating a given 'params' dict regarding a certain parameter.

    Checks whether the value of param is a dict and the key allowed or default is in the file.
    """

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            if not isinstance(self.value, dict):
                raise WrongTypeError(dict, type(self.value))
        except WrongTypeError as error:
            is_value_dict = Report(error)
            self.report = is_value_dict
            return True
        else:
            is_value_dict = Report("Value of param is a dict.")
            self.report = CompositeReport(is_value_dict)

            if "allowed" in self.value:
                has_allowed_key = Report("Optional key 'allowed' is in dict.")
                allowed_value = self.value.get("allowed")

                try:
                    if not isinstance(allowed_value, dict):
                        raise WrongTypeError(list, type(allowed_value))
                except WrongTypeError as error:
                    is_allowed_value_list = Report(error)
                    self.report.append_report(
                        CompositeReport(
                            has_allowed_key,
                            is_allowed_value_list
                        )
                    )
                else:
                    is_allowed_value_list = Report("Value of key 'allowed' is a dict.")

                    try:
                        if not allowed_value:
                            raise ValueError("Value of key 'allowed' is an empty list.")
                    except ValueError as error:
                        is_allowed_list_empty = Report(error)
                    else:
                        is_allowed_list_empty = Report("Value of key 'allowed' is a non-empty dict.")

                    self.report.append_report(
                        CompositeReport(
                            has_allowed_key,
                            is_allowed_value_list,
                            is_allowed_list_empty
                        )
                    )

            else:
                has_allowed_key = Report("Optional key 'allowed' is not in dict.")
                self.report.append_report(has_allowed_key)

            if "default" in self.value:
                has_default_key = Report("Optional key 'default' is in dict.")
                default_value = self.value.get("default")

                try:
                    if default_value is None:
                        raise ValueError("Value of key 'default' is None.")
                except ValueError as error:
                    is_default_value_none = Report(error)
                else:
                    is_default_value_none = Report("Value of key 'default' is not None.")
                self.report.append_report(
                    CompositeReport(
                        has_default_key,
                        is_default_value_none
                    )
                )

            else:
                has_default_key = Report("Optional key 'default' is not in dict.")
                self.report.append_report(has_default_key)

        return True


class ResponseValidator(CompositeValidator):
    """
    Validator for validating a given API method dict regarding the key 'response'.

    Checks whether 'response' exists in the dict and the value of the key is a dict.

    Attributes:
        value:
            The dict that shall be checked.
    """
    value: Dict[Text, Any]

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "response" not in self.value:
                raise KeyNotInDictError("response", self.value)
        except KeyNotInDictError as error:
            has_response = Report(error)
            self.report = has_response
        else:
            has_response = Report("Key 'response' exists.")
            response = self.value.get("response")

            try:
                if not isinstance(response, dict):
                    raise WrongTypeError(dict, type(response))
            except WrongTypeError as error:
                is_response_dict = Report(error)
                self.report = is_response_dict
            else:
                is_response_dict = Report("Value of key 'response' is a dict.")

                self.report = CompositeReport(has_response, is_response_dict)

        return True


class MappingValidator(CompositeValidator):
    """
    Validator for validating a given API method dict regarding the key 'mapping'.

    Checks whether 'mapping' exists in the dict and the value of the key is a list.

    Attributes:
        value:
            The dict that shall be checked.
    """
    value: Dict[Text, Any]

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "mapping" not in self.value:
                raise KeyNotInDictError("mapping", self.value)
        except KeyNotInDictError as error:
            has_mapping_key = Report(error)
            self.append_report(has_mapping_key)
            return True
        else:
            has_mapping_key = Report("Key 'mapping' exists.")
            self.append_report(has_mapping_key)

            mapping = self.value.get("mapping")

            try:
                if not isinstance(mapping, list):
                    raise WrongTypeError(list, type(mapping))
            except WrongTypeError as error:
                is_mapping_list = Report(error)
                self.append_report(is_mapping_list)
                return True
            else:
                is_mapping_list = Report("Value of key 'mapping' was a list.")
                self.append_report(is_mapping_list)

                for mapping_entry in mapping:
                    self.append_validator(MappingEntryValidator(mapping_entry))

                return super().validate()


class MappingEntryValidator(CompositeValidator):
    """
    Validator for validating a given API method dict regarding an entry of the key 'mapping'.

    Attributes:
        value:
            The dict that shall be checked.
    """
    value: Dict[Text, Any]

    def __init__(self, value: Any):
        """
        Constructor of MappingEntryValidator.

        @param value: The value to get validated.
        """
        super().__init__(
            value,
            KeyValidator(value),
            PathValidator(value),
            TypeValidator(value)
        )

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        try:
            if not isinstance(self.value, dict):
                raise WrongTypeError(dict, type(self.value))
        except WrongTypeError as error:
            is_mapping_dict = Report(error)
            self.append_report(is_mapping_dict)
            return True
        else:
            is_mapping_dict = Report("Mapping entry was a dict.")
            self.append_report(is_mapping_dict)

            return super().validate()


class KeyValidator(Validator):
    """
    Validator for validating a given 'map' dict regarding the key 'key'.

    Checks whether 'key' exists in the 'map' and the value of the key is a String.

    Attributes:
        value:
            The dict that shall be checked.
    """
    value: Dict[Text, Any]

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        mapping = self.value

        try:
            if "key" not in mapping:
                raise KeyNotInDictError("key", mapping)
        except KeyNotInDictError as error:
            has_key_key = Report(error)
            self.report = has_key_key
        else:
            has_key_key = Report("Key 'key' exists.")

            key_value = mapping.get("key")

            try:
                if not isinstance(key_value, str):
                    raise WrongTypeError(str, type(key_value))
            except WrongTypeError as error:
                is_key_str = Report(error)
                self.report = is_key_str
            else:
                is_key_str = Report("Value of key 'key' was a str.")

                self.report = CompositeReport(has_key_key, is_key_str)
        return True


class PathValidator(Validator):
    """
    Validator for validating a given 'map' dict regarding the key 'path'.

    Checks whether 'path' exists in the 'map' and the value of the key is a list.

    Attributes:
        value:
            The dict that shall be checked.
    """
    value: Dict[Text, Any]

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        mapping = self.value

        try:
            if "path" not in mapping:
                raise KeyNotInDictError("path", mapping)
        except KeyNotInDictError as error:
            has_path_key = Report(error)
            self.report = has_path_key
        else:
            has_path_key = Report("Key 'path' exists.")

            path_value = mapping.get("path")

            try:
                if not isinstance(path_value, list):
                    raise WrongTypeError(list, type(path_value))
            except WrongTypeError as error:
                is_path_list = Report(error)
                self.report = is_path_list
            else:
                is_path_list = Report("Value of key 'path' was a list.")

                self.report = CompositeReport(has_path_key, is_path_list)

        return True


class TypeValidator(Validator):
    """
    Validator for validating a given 'map' dict regarding the key 'type'.

    Checks whether 'type' exists in the 'map' and the value of the key is a list.

    Attributes:
        value:
            The dict that shall be checked.
    """
    value: Dict[Text, Any]

    def validate(self) -> bool:
        """
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """

        mapping = self.value

        try:
            if "type" not in mapping:
                raise KeyNotInDictError("type", mapping)
        except KeyNotInDictError as error:
            has_type_key = Report(error)
            self.report = has_type_key
        else:
            has_type_key = Report("Key 'type' exists.")

            type_value = mapping.get("type")

            try:
                if not isinstance(type_value, list):
                    raise WrongTypeError(list, type(type_value))
            except WrongTypeError as error:
                is_type_list = Report(error)
                self.report = is_type_list
            else:
                is_type_list = Report("Value of key 'type' was a list.")

                self.report = CompositeReport(has_type_key, is_type_list)

        return True


class RequestMappingValidator(Validator):
    """
    TODO: Fill out
    """
    value: Dict[Text, Any]

    @staticmethod
    def determine_table(request_name: str) -> dict:
        """
        Returns the database table based on the specified request name.

        @param request_name: The name of the request.

        @return: The database table based on the specified request name.
        """
        possible_class = {
            "currency_pairs":
                {"table": ExchangeCurrencyPair},
            "tickers":
                {"table": Ticker},
            "historic_rates":
                {"table": HistoricRate},
            "order_books":
                {"table": OrderBook},
            "trades":
                {"table": Trade}
        }
        return possible_class.get(request_name, lambda: "Invalid request class.")

    @staticmethod
    def determine_primary_keys(table_name: str) -> list:
        """
        Returns the primary keys based on the specified database table.

        @param table_name: The database table.

        @return: The primary keys based on the specified database table.
        """
        possible_primary_keys = {
            "currency_pairs":
                [],
            "tickers":
                ["time"],
            "historic_rates":
                ["time"],
            "order_books":
                ["position"],
            "trades":
                ["time"]
        }
        return possible_primary_keys.get(table_name, lambda: "Invalid request class")

    def validate(self) -> bool:
        # TODO: Philipp: Does not work, fix later
        self.report = Report("Need to be fixed later.")
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
        #                 is_type_list = Report(error)
        #                 self.report = is_type_list
        #     primary_keys = self.determine_primary_keys(request)
        #     for primary_key in primary_keys:
        #         if primary_key == requests[request]['mapping']:
        #             return False

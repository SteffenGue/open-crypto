#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Validator classes to validate the configuration file passed by the user. The module checks for existence, types and
values of necessary parameters. The file is executed in main.py before initializing the program.

Classes:
    - ConfigFileValidator(
        - ConfigYamlValidator(
            -> super().__init__(
            -    ConfigSectionValidator
            -    DatabaseStringValidator
            -    OperationSettingKeyValidator
            -    OperationSettingValueValidator
            -    UtilitiesValidator
            -    RequestKeysValidator
            -    RequestValueValidator
            )
        )
    )


"""

from typing import Any, Text, Dict, Union, Optional
from pandas import Interval
from typeguard import check_type

# pylint: disable=too-many-lines
from model.validating.api_map_validators import LoadFileValidator, LoadYamlValidator
from model.validating.base import Report, Validator, CompositeValidator
from model.validating.errors import KeyNotInDictError, WrongTypeError, WrongValueError, WrongCompositeValueError

# ToDo Implement typeguard for type-checking the config file using generic types.


class ConfigFileValidator(CompositeValidator):
    """Validator for an API Map file.

    Validator for validating a given configuration file.
    Consists of a FileLoadValidator, a YamlLoadValidator and an ConfigYamlValidator.

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

        config_file = ConfigYamlValidator(load_yaml.get_result_value())
        can_continue = config_file.validate()
        try:
            for report in config_file.report.reports:
                self.append_report(report)
        except (TypeError, Exception):
            self.append_report(config_file)

        return can_continue

    def result(self) -> bool:
        """
        TODO: Fill out
        """
        if not self.report:
            self.validate()

        return True


class ConfigYamlValidator(CompositeValidator):
    """Validator for loading Validators.

    Validator for calling the other required Validators.
    """

    def __init__(self, value: Dict[Text, Any]):
        """Constructor of ApiMapValidator.

        @param value:
            The value to get validated.
        """

        super().__init__(
            value,
            ConfigSectionValidator(value),
            DatabaseStringValidator(value['general']['database']),
            OperationSettingKeyValidator(value['general']['operation_settings']),
            OperationSettingValueValidator(value['general']['operation_settings']),
            UtilitiesValidator(value['general']['utilities']),
            RequestKeysValidator(value['jobs']),
            RequestValueValidator(value['jobs'])
        )


class ConfigSectionValidator(Validator):
    """
    Validates if all sections and blocks are present.

    """
    block = ['general', 'jobs']
    section = ['database', 'operation_settings', 'utilities']

    def validate(self) -> bool:

        try:

            for key in self.value:
                if key not in ConfigSectionValidator.block:
                    raise KeyNotInDictError(key, dict.fromkeys(ConfigSectionValidator.block))

            for key in self.value.get('general'):
                if key not in ConfigSectionValidator.section:
                    raise KeyNotInDictError(key, dict.fromkeys(ConfigSectionValidator.section))

        except KeyNotInDictError as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report(f"Configuration contains all blocks: {ConfigSectionValidator.block} and"
                                 f" sections: {ConfigSectionValidator.section}")
            return True


class DatabaseStringValidator(Validator):
    """
    Validates if all required database parameters exist to form the connection string.

    """
    db_strings = {'sqlite': ['sqltype', 'db_name'],
                  'mariadb': ['sqltype', 'username', 'password', 'host', 'port', 'db_name'],
                  'mysql': ['sqltype', 'username', 'password', 'host', 'port', 'db_name'],
                  'postgres': ['sqltype', 'username', 'password', 'host', 'port', 'db_name'],
                  }

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """

        try:

            if 'sqltype' not in self.value:
                raise KeyNotInDictError('sqltype', self.value)

            for key in DatabaseStringValidator.db_strings.get(self.value.get('sqltype')):

                if key not in self.value:
                    raise KeyNotInDictError(key, self.value)

                if self.value.get(key) is None:
                    raise WrongTypeError(str, type(self.value.get(key)), key)

        except (KeyNotInDictError, WrongTypeError) as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report("Database connection string is valid")
            return True


class OperationSettingKeyValidator(Validator):
    """
    Validates if all necessary keys exist in the section 'operational_settings'.

    """

    sections = {'frequency': Union[str, int, float],  # max 31 days
                'interval':  Optional[str],
                'timeout': Union[int, float],   # max 10 minutes
                }

    def validate(self) -> bool:
        """
        Validates the value.
        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """

        # Does key exist in config file
        try:
            for key, val in OperationSettingKeyValidator.sections.items():

                if key not in self.value:
                    raise KeyNotInDictError(key, dict.fromkeys(OperationSettingKeyValidator.sections))

                try:
                    check_type(key, self.value.get(key), val)
                except TypeError as error:
                    raise WrongTypeError(val, type(self.value.get(key))) from error

        except (KeyNotInDictError, WrongTypeError) as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report("Operation_settings have valid keys")
            return True


class OperationSettingValueValidator(Validator):
    """
    Validates if all necessary keys have correctly specified values in the section 'operational_settings'.

    """

    sections = {'frequency': Interval(0, 44640, "both"),  # max 31 days
                'interval': ['minutes', 'hours', 'days', 'weeks', 'months'],
                'timeout': Interval(0, 600, 'both'),  # max 10 minutes
                }

    def validate(self) -> bool:
        """
        Validates the value.
        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """

        try:
            for key, val in OperationSettingValueValidator.sections.items():

                if key == 'frequency' and isinstance(self.value.get(key), str):
                    if not self.value.get(key) == 'once':
                        raise WrongValueError(["once", val], self.value.get(key), key)
                    continue

                if not self.value.get(key) in val:
                    raise WrongValueError(val, self.value.get(key), key)

        except WrongValueError as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report("Operation settings are valid")
            return True


class UtilitiesValidator(Validator):
    """
    Validates if all necessary keys exist in the section 'utilities' and if the types are correct.

    """
    sections = {'enable_logging': Optional[bool],
                'yaml_path':  str}

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """
        try:
            for key, val in UtilitiesValidator.sections.items():

                if key not in self.value:
                    raise KeyNotInDictError(key, self.value)

                try:
                    check_type(key, self.value.get(key), val)
                except TypeError as error:
                    raise WrongTypeError(val, self.value.get(key), key) from error

        except (KeyNotInDictError, WrongTypeError) as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report('Utilities are valid')
            return True


class RequestKeysValidator(Validator):
    """
    Validates if all keys exist and types are correct for the request itself.

    """
    sections = {'yaml_request_name': str,
                'update_cp': Optional[bool],
                'exchanges': Optional[Union[list, str]],
                'excluded': Optional[Union[list, str]],
                'currency_pairs': Optional[list[dict[str, str]]],
                'first_currencies': Optional[Union[list, str]],
                'second_currencies': Optional[Union[list, str]],
                }

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """

        # if key is not nullable and not in the configuration file or empty
        try:
            for job in self.value:
                for key, val in RequestKeysValidator.sections.items():

                    if key not in self.value.get(job):
                        raise KeyNotInDictError(key, self.value.get(job))

                    try:
                        check_type(key, self.value.get(job).get(key), val)
                    except TypeError as error:
                        raise WrongTypeError(val.get('type'), self.value.get(job).get(key), key) from error

        except (KeyNotInDictError, WrongTypeError) as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report('Request keys and types are valid.')
            return True


class RequestValueValidator(Validator):
    """
    Validates if exchange and currency-pairs are specified and not None.
    """

    def validate(self) -> bool:
        try:
            for job in self.value:

                # if key 'currency-pairs' is specified
                if self.value.get(job).get("currency_pairs"):
                    for item in self.value.get(job).get('currency_pairs'):
                        if item == 'all':
                            continue
                        for key in ['first', 'second']:
                            if key not in item.keys() and item.keys:
                                raise KeyNotInDictError(key, item.keys())

                # if neither currency-pairs nor first_currencies or second_currencies are specified. That is only
                # allowed for the request method 'currency_pairs'.
                if self.value.get(job).get('yaml_request_name') == 'currency_pairs':
                    continue
                if all([self.value.get(job).get("currency_pairs") is None,
                        self.value.get(job).get("first_currencies") is None,
                        self.value.get(job).get("second_currencies") is None]):
                    raise WrongCompositeValueError(["currency_pairs", "first_currencies", "second_currencies"])

        except (WrongCompositeValueError, KeyNotInDictError) as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report("Currency-pairs are valid.")
            return True

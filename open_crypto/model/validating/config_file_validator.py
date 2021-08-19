#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
# ToDo: Module description and class-tree
"""

from typing import Any, Text, Dict
from pandas import Interval

# pylint: disable=too-many-lines
from model.validating.api_map_validators import LoadFileValidator, LoadYamlValidator
from model.validating.base import Report, CompositeReport, Validator, CompositeValidator
from model.validating.errors import KeyNotInDictError, WrongTypeError, WrongValueError, WrongCompositeValueError


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
            for r in config_file.report.reports:
                self.append_report(r)
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

        Args:
            value:
                The value to get validated.
        """

        super().__init__(
            value,
            ConfigSectionValidator(value),
            DatabaseStringValidator(value['general']['database']),
            OperationSettingValidator(value['general']['operation_settings']),
            UtilitiesValidator(value['general']['utilities']),
            RequestKeysValidator(value['jobs']),
            RequestValueValidator(value['jobs'])
        )


class ConfigSectionValidator(Validator):
    """
    Validator for ...

    """
    block = ['general', 'jobs']
    section = ['database', 'operation_settings', 'utilities']

    def validate(self) -> bool:

        try:
            for key in self.value:
                if key not in ConfigSectionValidator.block:
                    raise KeyNotInDictError(key, dict.fromkeys(ConfigSectionValidator.block))
        except KeyNotInDictError as error:
            self.report = Report(error)
            return False

        try:
            for key in self.value.get('general'):
                if key not in ConfigSectionValidator.section:
                    raise KeyNotInDictError(key, dict.fromkeys(ConfigSectionValidator.section))
        except KeyNotInDictError as error:
            self.report = CompositeReport(has_blocks, Report(error))
            return False
        else:
            self.report = Report(f"Configuration contains all blocks {ConfigSectionValidator.block} and"
                                 f" sections: {ConfigSectionValidator.section}")

        return True


class DatabaseStringValidator(Validator):
    """
    Validator for ...
    # ToDo
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
        except KeyNotInDictError as error:
            self.report = Report(error)
            return False

        try:
            for key in DatabaseStringValidator.db_strings.get(self.value.get('sqltype')):
                if key not in self.value:
                    raise KeyNotInDictError(key, self.value)
        except KeyNotInDictError as error:
            self.report = CompositeReport(sqltype, Report(error))
            return False

        try:
            for key in DatabaseStringValidator.db_strings.get(self.value.get("sqltype")):
                if self.value.get(key) is None:
                    raise WrongTypeError(str, type(self.value.get(key)))
        except WrongTypeError as error:
            self.report = CompositeReport(sqltype, db_string, Report(error))
            return False
        else:
            self.report = Report("Database connection string is valid")
            return True


class OperationSettingValidator(Validator):
    """
    # ToDo
    """

    sections = {'frequency': {'type': (str, int, float), 'values': ['once', Interval(0, 44640, "both")]},  # max 31 days
                'interval': {'type': (str, type(None)), 'values': ['minutes', 'hours', 'days', 'weeks', 'months']},
                'timeout': {'type': (int, float), 'values': [Interval(0, 600, 'both')]}  # max 10 minutes
                }

    def validate(self) -> bool:
        """
        Validates the value.
        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """

        # Does key exist in cofig file
        try:
            for key, val in OperationSettingValidator.sections.items():

                if key not in self.value:
                    raise KeyNotInDictError(key, dict.fromkeys(OperationSettingValidator.sections))

                if not isinstance(self.value.get(key), val.get('type')):
                    raise WrongTypeError(val.get('type'), type(self.value.get(key)))

        except KeyNotInDictError as error:
            self.report = Report(error)
            return False

        except WrongTypeError as error:
            self.report = CompositeReport(sections_keys, Report(error))
            return False

        else:
            self.report = Report("Operation_settings are valid")
            return True


class UtilitiesValidator(Validator):
    """
    Validator for the ??.

    """
    sections = {'enable_logging': {'type': bool},
                'yaml_path': {'type': str}}

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """
        try:
            for key in UtilitiesValidator.sections:
                if key not in self.value:
                    raise KeyNotInDictError(key, self.value)
                if not isinstance(self.value.get(key), UtilitiesValidator.sections.get(key).get('type')):
                    raise WrongTypeError(OperationSettingValidator.sections.get(key).get('type'),
                                         self.value.get(key))

        except (KeyNotInDictError, WrongTypeError) as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report('Utilities are valid')

        return True


class RequestKeysValidator(Validator):
    """
    Validator for the ??.

    """
    sections = {'yaml_request_name': {'type': str},
                'update_cp': {'type': (bool, type(None))},
                'exchanges': {'type': list},
                'excluded': {'type': (list, type(None))},
                'currency_pairs': {'type': (list, type(None))},
                'first_currencies': {'type': (list, type(None))},
                'second_currencies': {'type': (list, type(None))},
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

                    if not isinstance(self.value.get(job).get(key), val.get('type')):
                        raise WrongTypeError(val.get('type'), self.value.get(job).get(key))

        except KeyNotInDictError as error:
            self.report = Report(error)
            return False
        except WrongTypeError as error:
            self.report = CompositeReport(all_keys_exist, Report(error))
            return False

        # if there exist any currency-pairs to request
        try:
            for job in self.value:
                if self.value.get(job).get('yaml_request_name') == 'currency_pairs':
                    continue
                if all([self.value.get(job).get("currency_pairs") is None,
                        self.value.get(job).get("first_currencies") is None,
                        self.value.get(job).get("second_currencies") is None]):

                    raise WrongCompositeValueError(["currency_pairs", "first_currencies", "second_currencies"])

        except WrongCompositeValueError as error:
            self.report = CompositeReport(all_keys_exist,
                                          key_types,
                                          Report(error))
            return False
        else:
            self.report = Report('Request keys and types are valid.')

        return True


class RequestValueValidator(Validator):
    """
    # ToDo
    """
    def validate(self) -> bool:
        try:
            for job in self.value:
                if self.value.get(job).get("currency_pairs"):
                    for item in self.value.get(job).get('currency_pairs'):
                        if item == 'all':
                            continue
                        for key in ['first', 'second']:
                            if key not in item.keys() and item.keys:
                                raise KeyNotInDictError(key, item.keys())

        except KeyNotInDictError as error:
            self.report = Report(error)
            return False
        else:
            self.report = Report("Currency-pairs are valid.")

        return True

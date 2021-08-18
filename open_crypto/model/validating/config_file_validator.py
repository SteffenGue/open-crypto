#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
# ToDo: Module description and class-tree
"""

from typing import Any, Text, Dict

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
        else:
            has_blocks = Report("All blocks contained in the config file.")

        try:
            for key in self.value.get('general'):
                if key not in ConfigSectionValidator.section:
                    raise KeyNotInDictError(key, dict.fromkeys(ConfigSectionValidator.section))
        except KeyNotInDictError as error:
            self.report = CompositeReport(has_blocks, Report(error))
            return False
        else:
            self.report = Report("All blocks in configuration file exist.")

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
        else:
            sqltype = Report("Sqltype in keys.")

        try:
            for key in DatabaseStringValidator.db_strings.get(self.value.get('sqltype')):
                if key not in self.value:
                    raise KeyNotInDictError(key, self.value)
        except KeyNotInDictError as error:
            self.report = CompositeReport(sqltype, Report(error))
            return False
        else:
            db_string = Report("All keys for database connection string found.")

        try:
            for key in DatabaseStringValidator.db_strings.get(self.value.get("sqltype")):
                if self.value.get(key) is None:
                    raise WrongTypeError(str, type(self.value.get(key)))
        except WrongTypeError as error:
            self.report = CompositeReport(sqltype, db_string, Report(error))
            return False
        else:
            self.report = Report("Database section is valid")

        return True


class OperationSettingValidator(Validator):
    """
    # ToDo
    """

    sections = {'frequency': {'type': (str, int, float), 'values': ['once', *range(0, 44640)]},  # max 31 days
                'interval': {'type': str, 'values': ['minutes', 'hours', 'days', 'weeks', 'months']},
                'timeout': {'type': (int, float), 'values': [*range(0, 600)]}  # max 10 minutes
                }

    def validate(self) -> bool:
        """
        Validates the value.
        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """

        try:
            for key in OperationSettingValidator.sections:
                if key not in self.value:
                    raise KeyNotInDictError(key, dict.fromkeys(ConfigSectionValidator.block))
        except KeyNotInDictError as error:
            self.report = Report(error)
            return False
        else:
            sections_keys = Report("All keys for operational_settings exist.")

        try:
            for key in OperationSettingValidator.sections:
                if not isinstance(self.value.get(key), OperationSettingValidator.sections.get(key).get('type')):
                    raise WrongTypeError(OperationSettingValidator.sections.get(key).get('type'),
                                         type(self.value.get(key)))

                if not self.value.get(key) in OperationSettingValidator.sections.get(key).get('values'):
                    raise WrongValueError(OperationSettingValidator.sections.get(key).get('values'),
                                          self.value.get(key))
        except (WrongTypeError, WrongValueError) as error:
            self.report = CompositeReport(sections_keys, Report(error))
            return False
        else:
            self.report = Report("Block operation_settings is valid.")

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
            self.report = Report('Block utilities is valid.')

        return True


class RequestKeysValidator(Validator):
    """
    Validator for the ??.

    """
    sections = {'yaml_request_name': {'type': str},
                'update_cp': {'type': bool, 'nullable': True},
                'exchanges': {'type': list},
                'excluded': {'type': list, 'nullable': True},
                'currency_pairs': {'type': list, 'nullable': True},
                'first_currencies': {'type': list, 'nullable': True},
                'second_currencies': {'type': list, 'nullable': True},
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
                for key in RequestKeysValidator.sections:
                    if not RequestKeysValidator.sections.get(key).get("nullable", False) \
                            and (key not in self.value.get(job).keys() or not self.value.get(job).get(key)):
                        raise KeyNotInDictError(key, self.value.get(job))
        except KeyNotInDictError as error:
            self.report = Report(error)
            return False
        else:
            all_keys_exist = Report("All request keys exist.")

        # if types of configuration file values exist but are not allowed
        try:
            for job in self.value:
                for key, val in RequestKeysValidator.sections.items():
                    if self.value.get(job).get(key) and not \
                            isinstance(self.value.get(job).get(key), val.get('type')):
                        raise WrongTypeError(val.get('type'),
                                             self.value.get(job).get(key))
        except WrongTypeError as error:
            self.report = CompositeReport(all_keys_exist, Report(error))
            return False
        else:
            key_types = Report("All types of existing keys are valid.")

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
            self.report = Report("Values of currency-pair keys are valid.")

        return True

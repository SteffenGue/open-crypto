#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Validator classes to validate the configuration file passed by the user. The module checks for existence, types and
values of necessary parameters. The file is executed in main.py before initializing the program.

Classes:
    - ConfigFileValidator(
        - ConfigYamlValidator(
            -> super().__init__(

            -    ConfigSectionValidator(value),
            -    ProgramSettingKeyValidator(value),
            -    ProgramSettingValueValidator(value)
            )
        )
    )
"""

from typing import Any, Text, Dict, Union

from pandas import Interval
from typeguard import check_type

# pylint: disable=too-many-lines
from model.validating.api_map_validators import LoadFileValidator, LoadYamlValidator
from model.validating.base import Report, Validator, CompositeValidator
from model.validating.errors import KeyNotInDictError, WrongTypeError, WrongValueError


# ToDo Debug
class ProgramConfigValidator(CompositeValidator):
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

        config_file = ProgramConfigYamlValidator(load_yaml.get_result_value())
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


class ProgramConfigYamlValidator(CompositeValidator):
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
            ProgramSettingKeyValidator(value),
            ProgramSettingValueValidator(value)
        )


class ConfigSectionValidator(Validator):
    """
    Validates if all sections and blocks are present.

    """
    blocks = ["logging", "request_settings"]
    sections = ["dirname", "filename_format", "level", "exception_hook",
                "min_return_tuples", "interval_settings"]

    def validate(self) -> bool:

        try:

            for key in self.value:
                if key not in ConfigSectionValidator.blocks:
                    raise KeyNotInDictError(key, dict.fromkeys(ConfigSectionValidator.blocks))

            for block in ConfigSectionValidator.blocks:
                for key in self.value.get(block).keys():
                    if key not in ConfigSectionValidator.sections:
                        raise KeyNotInDictError(key, dict.fromkeys(ConfigSectionValidator.sections))

        except KeyNotInDictError as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report(f"Configuration contains all blocks: {ConfigSectionValidator.blocks} and"
                                 f" sections: {ConfigSectionValidator.sections}")
            return True


class ProgramSettingKeyValidator(Validator):
    """
    Validates if all necessary keys exist in the section 'operational_settings'.
    """

    sections = {"dirname": str,
                "filename_format": str,
                "level": str,
                "exception_hook": Union[int, bool],
                "min_return_tuples": int,
                "interval_settings": str,
                }

    def validate(self) -> bool:
        """
        Validates the value.
        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """

        # Does key exist in config file
        try:
            merged_values = dict()
            for item in self.value.keys():
                merged_values.update(self.value.get(item))

            for key, val in ProgramSettingKeyValidator.sections.items():
                try:
                    check_type(key, merged_values.get(key), val)
                except TypeError as error:
                    raise WrongTypeError(val, type(self.value.get(key))) from error

        except (KeyNotInDictError, WrongTypeError) as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report("Program settings have valid types")
            return True


class ProgramSettingValueValidator(Validator):
    """
    Validates if all necessary keys have correctly specified values in the section 'operational_settings'.

    """
    sections = {"level": ["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                "min_return_tuples": Interval(0, 1000000, "both"),
                "interval_settings": ["equal", "lower", "higher", "lower_or_equal", "high_or_equal"],
                }

    def validate(self) -> bool:
        """
        Validates the value.

        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """

        try:
            merged_values = dict()
            for item in self.value.keys():
                merged_values.update(self.value.get(item))

            for key, val in ProgramSettingValueValidator.sections.items():
                if not merged_values.get(key) in val:
                    raise WrongValueError(val, merged_values.get(key), key)

        except WrongValueError as error:
            self.report = Report(error)
            return False

        else:
            self.report = Report("Program settings are valid")
            return True

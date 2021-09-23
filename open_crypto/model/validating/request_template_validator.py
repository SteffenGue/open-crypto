#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Validators to validate the request template.

The module checks for existence, types and values of necessary parameters.

Validators structure:

- RequestTemplateValidator
  - LoadFileValidator
  - LoadYamlValidator
  - RequestTemplateCoreValidator
    - ConfigSectionValidator
    - DatabaseStringValidator
    - OperationSettingKeyValidator
    - RequestKeysValidator
"""
import sys
from typing import Any, Text, Dict

from model.validating.api_map_validators import LoadFileValidator, LoadYamlValidator
from model.validating.base import Report, Validator, CompositeValidator
from model.validating.errors import KeyNotInDictError, WrongTypeError, WrongValueError


class RequestTemplateValidator(CompositeValidator):
    """
    Validator for an request template.

    Attributes:
        - value: The file name or file path as Text.
    """
    value: Text

    def validate(self) -> bool:
        """
        Validates the value.

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

        request_template = RequestTemplateCoreValidator(load_yaml.get_result_value())
        can_continue = request_template.validate()
        try:
            for report in request_template.report.reports:
                self.append_report(report)
        except (TypeError, Exception):
            self.append_report(request_template)

        return can_continue

    def result(self) -> bool:
        """
        TODO: Fill out
        """
        if not self.report:
            self.validate()

        return True


class RequestTemplateCoreValidator(CompositeValidator):
    """
    Validator for the contents of an request template.
    """

    def __init__(self, value: Dict[Text, Any]):
        """
        Constructor of RequestTemplateCoreValidator.

        @param value: The value to get validated.
        """
        super().__init__(
            value,
            ConfigSectionValidator(value),
            DatabaseStringValidator(value["general"]["database"]),
            OperationSettingKeyValidator(value["general"]["operation_settings"]),
            RequestKeysValidator(value["jobs"])
        )


class ConfigSectionValidator(Validator):
    """
    Validator to check if all sections and blocks are present.
    """
    block = ["general", "jobs"]
    section = ["database", "operation_settings", "utilities"]

    def validate(self) -> bool:
        try:
            for key in self.value:
                if key not in ConfigSectionValidator.block:
                    raise KeyNotInDictError(key, dict.fromkeys(ConfigSectionValidator.block))

            for key in self.value.get("general"):
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
    Validator to check if the database block is valid.
    """
    db_strings = (
        ("sqltype", str, "sqlite"),
        ("client", (str, type(None)), None),
        ("user_name", (str, type(None)), None),
        ("password", (str, type(None)), None),
        ("host", str, "localhost"),
        ("port", int, 5432),
        ("db_name", str, "ExampleDB")
    )

    def validate(self) -> bool:
        """
        Validates the value.

        @return: Whether further Validators may continue validating.
        """
        try:
            for (key, ex_type, ex_value) in DatabaseStringValidator.db_strings:
                if key not in self.value:
                    raise KeyNotInDictError(key, self.value)

                if not isinstance(self.value[key], ex_type):
                    raise WrongTypeError(ex_type, type(self.value[key]), key)

                if self.value[key] != ex_value:
                    raise WrongValueError(ex_value, self.value[key], key)
        except (KeyNotInDictError, WrongTypeError) as error:
            self.report = Report(error)
            return False
        else:
            self.report = Report("Database connection string is valid")
            return True


class OperationSettingKeyValidator(Validator):
    """
    Validator to check if the operation settings block is valid.
    """
    db_strings = (
        ("frequency", str, "once"),
        ("interval", str, "days"),
        ("timeout", int, 10),
        ("enable_logging", bool, True),
        ("asynchronously", bool, True)
    )

    def validate(self) -> bool:
        """
        Validates the value.

        @return: Whether further Validators may continue validating.
        """
        try:
            for (key, ex_type, ex_value) in OperationSettingKeyValidator.db_strings:
                if key not in self.value:
                    raise KeyNotInDictError(key, self.value)

                if not isinstance(self.value[key], ex_type):
                    raise WrongTypeError(ex_type, type(self.value[key]), key)

                if self.value[key] != ex_value:
                    raise WrongValueError(ex_value, self.value[key], key)
        except (KeyNotInDictError, WrongTypeError) as error:
            self.report = Report(error)
            return False
        else:
            self.report = Report("Operation settings are valid")
            return True


class RequestKeysValidator(Validator):
    """
    Validates if all keys exist and types are correct for the request itself.
    """
    sections = (
        ("request_method", type(None), None),
        ("update_cp", bool, False),
        ("excluded", type(None), None),
        ("exchanges", type(None), None),
        ("currency_pairs", type(None), None),
        ("first_currencies", type(None), None),
        ("second_currencies", type(None), None),
    )

    def validate(self) -> bool:
        """
        Validates the value.

        @return: Whether further Validators may continue validating.
        """
        try:
            if "JobName" not in self.value:
                raise KeyNotInDictError("JobName", self.value)

            job = self.value["JobName"]
            for (key, ex_type, ex_value) in RequestKeysValidator.sections:
                if key not in job:
                    raise KeyNotInDictError(key, job)

                if not isinstance(job[key], ex_type):
                    raise WrongTypeError(ex_type, type(job[key]), key)

                if job[key] != ex_value:
                    raise WrongValueError(ex_value, job[key], key)
        except (KeyNotInDictError, WrongTypeError, WrongValueError) as error:
            self.report = Report(error)
            return False
        else:
            self.report = Report("Request keys and types are valid.")
            return True


if __name__ == "__main__":
    IS_VALID = RequestTemplateValidator("open_crypto/resources/templates/request_template.yaml").validate()
    sys.exit(int(not IS_VALID))

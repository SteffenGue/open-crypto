from typing import Any, Text, Dict

import oyaml as yaml
import validators

# pylint: disable=too-many-lines
from model.validating.api_map_validators import LoadFileValidator, LoadYamlValidator
from model.validating.base import Report, CompositeReport, Valid, Validator, CompositeValidator, ProcessingValidator
from model.validating.errors import KeyNotInDictError, SubstringNotInStringError, WrongTypeError, UrlValidationError, \
    NamingConventionError


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

        return


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
            DatabaseStringValidator(value),
            # OperationSettingValidator(value),
            # RequestJobValidator(value),
        )


class ConfigSectionValidator(Validator):
    """
    Validator for ...
    # ToDo
    """
    def validate(self) -> bool:
        keys_to_check = ['general', 'jobs']
        try:
            for key in self.value.keys():
                if key not in keys_to_check:
                    raise KeyNotInDictError(key, dict.fromkeys(keys_to_check))
        except KeyNotInDictError as error:
            has_section = Report(Valid(error))
            self.report = has_section
        else:
            has_section = Report(Valid("All keys contained in the config file."))
            self.report = has_section

        return True


class DatabaseStringValidator(Validator):
    """
    Validator for ...
    # ToDo
    """

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """

        try:
            db_section = self.value.get("general").\
                get('database', KeyNotInDictError('database', self.value.get("general")))
        except KeyNotInDictError as error:
            has_keys = Report(Valid(error))
            self.report = has_keys
        else:
            has_keys = Report(Valid("Database Section found."))
            self.report = has_keys

        try:
            if db_section.get("sqltype", KeyNotInDictError("sql_type", db_section)) == 'sqlite':
                necessary_keys = ['sqltype', 'db_name']
                for key in necessary_keys:
                    db_section.get(key, WrongTypeError('str', type(db_section.get(key))))

            if db_section.get("postgres", KeyNotInDictError("sql_type", db_section)) == 'postgres':
                necessary_keys = ['sqltype', 'client', 'user_name', 'password', 'host', 'port']
                for key in necessary_keys:
                    db_section.get(key, WrongTypeError('str', type(db_section.get(key))))

            if db_section.get("mariadb", KeyNotInDictError("sql_type", db_section)) == 'mariadb':
                necessary_keys = ['sqltype', 'client', 'user_name', 'password', 'host', 'port']
                for key in necessary_keys:
                    db_section.get(key, WrongTypeError('str', type(db_section.get(key))))

        except (KeyNotInDictError, WrongTypeError) as error:
            has_keys = Report(Valid(error))
            self.report = has_keys

        else:
            has_keys = Report(Valid("Database string is complete."))
            self.report = has_keys


class OperationSettingValidator(Validator):
    """
    # ToDo
    Validator for ...
    """

    def validate(self) -> bool:
        """
        Validates the value.
        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """
        return True


class RequestJobValidator(Validator):
    """
    Validator for the ??.

    """

    def validate(self) -> bool:
        """Validates the value.

        Validates the value attribute while generating a validation Report.

        @return: bool
            Whether further Validators may continue validating.
        """
        return True

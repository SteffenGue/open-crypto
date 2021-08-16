#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Contains base validators and reports to represent validation results.
"""

from __future__ import annotations

import abc
import re
import textwrap
from typing import Text, Union, Any, Optional


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
    report: Optional[Report]

    def __init__(self, value: Union[Any, Validator]):
        """
        Constructor of Validator Base class.

        @param value: The value to get validated or a validator having the result value.
        """
        if isinstance(value, Validator):
            self.value = value.get_result_value()
        else:
            self.value = value

        self.report = None

    def get_result_value(self) -> Any:
        """
        Returns the value of the validator.

        @return: The value of the Validator.
        """
        return self.value

    @abc.abstractmethod
    def validate(self) -> bool:
        """
        Validates the value.

        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        raise NotImplementedError

    def __bool__(self) -> bool:
        """
        @return: Returns the boolean value of its validation report.
        """
        return bool(self.report)


class CompositeValidator(Validator):
    """Composite Validator.

    CompositeValidator containing a list of Validators while being
    a Validator itself.

    Attributes:
        validators:
            A List of children Validators.
    """
    validators: list[Validator]

    def __init__(self, value: Any, *child_validators: Validator):
        """
        Constructor of CompositeValidator.

        @param value: The value to get validated.
        @param child_validators: A variable-length sequence of children Validators.
        """
        super().__init__(value)

        self.validators = list(child_validators)
        self.report = CompositeReport()

    def append_validator(self, validator: Validator) -> None:
        """
        Appends a validator to validators.

        @param validator: The validator to get appended to list of validators.
        """
        self.validators.append(validator)

    def append_report(self, report: Union[Validator, Report]) -> None:
        """
        Appends a Report.

        Appends a Report or the Report of a Validator to the CompositeReport.

        @param report: A Validator whose Report will get appended or a Report.
        """
        assert isinstance(self.report, CompositeReport)
        self.report.append_report(report)

    def get_result_value(self) -> Any:
        """
        Returns the value or result.

        Returns its value if no special Validator is the last of its children.
        Otherwise the result value of the last Validator is returned.

        @return: The value or result value.
        """
        last_validator = self.validators[-1]

        if isinstance(last_validator, (ProcessingValidator, CompositeValidator)):
            return last_validator.get_result_value()

        return self.value

    def validate(self) -> bool:
        """
        Validates the value.

        Validates the value attribute while generating a validation Report.

        @return: Whether further Validators may continue validating.
        """
        assert isinstance(self.report, CompositeReport)

        for validator in self.validators:
            is_valid = validator.validate()
            self.report.append_report(validator.report)

            if not is_valid:
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


class Valid:
    """
    Wrapper class for Exception or Text

    Wrapper class for wrapping Exceptions or Text while providing a
    boolean value (False for Exceptions, True otherwise).

    Attributes:
        message:
            An Exception instance or a Text message (str).
    """
    message: Union[Exception, Text]

    def __init__(self, message: Union[Exception, Text]):
        """
        Construct a Valid object wrapping the specified message.

        @param message: An exception or text message to be wrapped.
        """
        self.message = message

    def __bool__(self) -> bool:
        """
        @return: False if a exception is wrapped, True otherwise.
        """
        return not isinstance(self.message, Exception)

    def __repr__(self) -> Text:
        """
        A method for representing a message in text format.

        A text value returning "-" and the respective message, if message is a
        Exception, otherwise "+" and the respective message.

        @return: "-" for Exceptions, "+" otherwise.
        """
        sign = "+" if self else "-"
        return sign + " " + repr(self.message)


class Report:
    """
    Report of Validation.

    Report of the Validation carried out in a Validator.
    Contains a list of Exceptions and Messages collected during Validation.

    Attributes:
        messages:
            List of wrapped Exceptions or Messages.
    """
    messages: list[Valid]

    def __init__(self, *messages: Valid):
        """
        Constructor of Report.

        @param messages: A variable-length sequence of Valid objects.
        """
        self.messages = list(messages)

    def indented_report(self) -> Text:
        """
        Indents the report.

        Indents the generated report representation according to the
        opening and closing brackets.

        @return: The indented representation.
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

    def print_report(self) -> None:
        """
        Prints the Report's indented representation.
        """
        print(self.indented_report())

    def __bool__(self) -> bool:
        """
        A boolean value.

        A boolean value returning True if all elements in messages are True,
        otherwise returning False.

        @return: True if all messages are True, False otherwise.
        """
        return all(self.messages)

    def __repr__(self) -> Text:
        """
        A method for representing a message in text format.

        A text value returning "-" and the respective message, if message is a
        Exception, otherwise "+" and the respective message.

        @return: "-" for Exceptions, "+" otherwise.
        """
        if len(self.messages) == 1:
            return repr(self.messages[0])

        sign = "+" if self else "-"
        return sign + " " + repr(self.messages)


class CompositeReport(Report):
    """
    CompositeReport consisting of multiple Reports.

    Attributes:
        reports:
            A list of child Reports.
    """
    reports: list[Report]

    def __init__(self, *reports: Report):
        """
        Constructor of CompositeReport.

        @param reports: A variable-length sequence of Report objects.
        """
        super().__init__()

        self.reports = list(reports)

    def append_report(self, report: Union[Report, Validator]) -> None:
        """
        Appends a Report to reports.

        Appends a Report or a Validator's Report to children reports.

        @param report: A Report or Validator containing a Report.
        """
        if isinstance(report, Validator):
            report = report.report

        self.reports.append(report)

    def __bool__(self) -> bool:
        """
        A boolean value.

        A boolean value returning True if all elements in reports are True,
        otherwise returning False.

        @return: True if all reports are True, False otherwise.
        """
        return all(self.reports)

    def __repr__(self) -> Text:
        """
        A method for representing a report in text format.

        A text value returning "-" and the respective report, if report is a
        Exception, otherwise "+" and the respective report.

        @return: "-" for Exceptions, "+" otherwise.
        """
        sign = "+" if self else "-"
        return sign + " " + repr(self.reports)

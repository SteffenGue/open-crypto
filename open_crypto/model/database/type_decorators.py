#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Contains custom database types.

Classes:
 - UnixTimestampMs: Database type to store dates and times as Unix timestamps in milliseconds (UTC+0).
"""

from datetime import datetime
from typing import Type, Optional

from sqlalchemy.engine import Dialect
from sqlalchemy.types import BIGINT, TypeDecorator

from model.utilities.time_helper import TimeHelper, TimeUnit


class UnixTimestampMs(TypeDecorator):
    """
    Database type to store Unix timestamps in milliseconds (UTC+0).

    Although it is stored as an integer internally, it is used like a timezone aware (UTC+0) datetime.
    """
    impl = BIGINT

    cache_ok = True

    @property
    def python_type(self) -> Type[datetime]:
        return datetime

    def process_bind_param(self, value: datetime, dialect: Dialect) -> Optional[int]:
        if value is None:
            return None

        return int(TimeHelper.to_timestamp(value, TimeUnit.MILLISECONDS))

    def process_result_value(self, value: int, dialect: Dialect) -> Optional[datetime]:
        if value is None:
            return None

        return TimeHelper.from_timestamp(value, TimeUnit.MILLISECONDS)

    def process_literal_param(self, value: int, dialect: Dialect) -> str:
        pass

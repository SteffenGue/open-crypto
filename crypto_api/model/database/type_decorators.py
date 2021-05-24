from datetime import datetime
from typing import Type, Optional

from sqlalchemy.engine import Dialect
from sqlalchemy.types import BIGINT, TypeDecorator

from model.utilities.time_helper import TimeHelper, TimeUnit


class UnixTimestamp(TypeDecorator):
    """
    Datatype to store Unix timestamps in a database.

    Although it's stored as BIGINT internally, it's returned as timezone aware (UTC+0) datetime.
    """
    impl = BIGINT

    @property
    def python_type(self) -> Type[datetime]:
        return datetime

    def process_bind_param(self, value: datetime, dialect: Dialect) -> int:
        return int(TimeHelper.to_timestamp(value, TimeUnit.MILLISECONDS))

    def process_result_value(self, value: int, dialect: Dialect) -> Optional[datetime]:
        if value is None:
            return None

        return TimeHelper.from_timestamp(value, TimeUnit.MILLISECONDS)

    def process_literal_param(self, value, dialect):
        pass  # TODO: Abstract method from super class. Fix if it causes trouble...

from datetime import datetime

from sqlalchemy.types import INTEGER, TypeDecorator

from model.utilities.time_helper import TimeHelper, TimeUnit


class UnixTimestamp(TypeDecorator):
    """
    Datatype to store Unix timestamps in a database.

    Although it's stored as INTEGER internally, it's returned as timezone aware (UTC+0) datetime.
    """
    impl = INTEGER

    @property
    def python_type(self):
        return datetime

    def process_bind_param(self, value, dialect):
        return TimeHelper.to_timestamp(value, TimeUnit.SECONDS)

    def process_result_value(self, value, dialect):
        if value is None:
            return None

        return TimeHelper.from_timestamp(value, TimeUnit.SECONDS)

    def process_literal_param(self, value, dialect):
        pass  # TODO: Improve if this method causes any kind of trouble.

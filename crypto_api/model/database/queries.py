#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TODO: Fill out module docstring.
"""

from sqlalchemy import func
from sqlalchemy.orm import Query


def fast_count(query: Query) -> int:
    """
    TODO: Fill out
    """
    count_q = query.statement.with_only_columns([func.count()]).order_by(None)
    count = query.session.execute(count_q).scalar()
    return int(count)  # TODO: Is int casting allowed here?

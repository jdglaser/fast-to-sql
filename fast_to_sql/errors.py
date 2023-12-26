"""Custom errors for fast_to_sql
"""


class FailError(Exception):
    pass


class InvalidParamError(Exception):
    pass


class DuplicateColumns(Exception):
    pass


class CustomColumnException(Exception):
    pass

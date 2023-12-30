"""Custom errors for fast_to_sql
"""


class FailError(Exception):
    pass


class InvalidParamError(Exception):
    pass


class DuplicateColumnsException(Exception):
    pass


class CustomColumnException(Exception):
    pass

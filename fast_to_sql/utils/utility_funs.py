# Global
from typing import Dict, Literal

import pyodbc
from pandas import DataFrame

from fast_to_sql.errors import CustomColumnException, DuplicateColumnsException, InvalidParamError

DTYPE_MAP = {
    "int64": "bigint",
    "int32": "int",
    "int16": "smallint",
    "int8": "tinyint",
    "float64": "float",
    "object": "nvarchar(255)",
    "datetime64[ns]": "datetime2",
    "bool": "bit",
}


def check_duplicate_cols(df: DataFrame):
    """Returns duplicate column names (case insensitive)"""
    cols = [c.lower() for c in df.columns]
    dups = [x for x in cols if cols.count(x) > 1]
    if dups:
        raise DuplicateColumnsException(
            f"There are duplicate column names. Repeated names are: {dups}. SQL Server dialect requires unique names "
            f"(case insensitive)."
        )


def clean_col_name(column: str):
    """Removes special characters from column names"""
    column = str(column).replace(" ", "_").replace("(", "").replace(")", "").replace("[", "").replace("]", "")
    column = f"[{column}]"
    return column


def clean_custom(df: DataFrame, custom: Dict[str, str]):
    """Validate and clean custom columns"""
    new_custom: Dict[str, str] = {}
    for k in custom:
        clean_col = clean_col_name(k)
        if clean_col not in df.columns:
            raise CustomColumnException(f"Custom column {k} is not in the dataframe.")
        new_custom[clean_col] = custom[k]
    return new_custom


def get_data_types(df: DataFrame, custom: Dict[str, str]):
    """Get data types for each column as dictionary
    Handles default data type assignment and custom data types
    """
    data_types: Dict[str, str] = {}
    for c in list(df.columns):
        if c in custom:
            data_types[c] = custom[c]
            continue
        dtype = str(df[c].dtype)
        if dtype not in DTYPE_MAP:
            data_types[c] = "varchar(255)"
        else:
            data_types[c] = DTYPE_MAP[dtype]
    return data_types


def get_default_schema(cur: pyodbc.Cursor) -> str:
    """Get the default schema of the caller"""
    return str(cur.execute("select SCHEMA_NAME() as scm").fetchall()[0][0])


def get_schema(cur: pyodbc.Cursor, table_name: str):
    """Get schema and table name - returned as tuple"""
    t_spl = table_name.split(".")
    if len(t_spl) > 1:
        return t_spl[0], ".".join(t_spl[1:])
    else:
        return get_default_schema(cur), table_name


def clean_table_name(table_name: str, temp: bool = False):
    """Cleans the table name"""
    if temp is True and table_name.find("#") != 0:
        return "#" + table_name.replace("'", "''")
    else:
        return table_name.replace("'", "''")


def check_exists(cur: pyodbc.Cursor, schema: str, table: str, temp: bool) -> bool:
    """Check in conn if table exists"""
    if temp:
        res = cur.execute(f"IF OBJECT_ID('tempdb..[{table}]') IS NOT NULL select 1 else select 0").fetchall()[0][0]
        return True if res == 1 else False
    else:
        res = cur.execute(
            f"IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}' and TABLE_SCHEMA = "
            f"'{schema}') select 1 else select 0"
        ).fetchall()[0][0]
        return True if res == 1 else False


def generate_create_statement(schema: str, table: str, cols: Dict[str, str], temp: bool):
    """Generates a create statement"""
    col_list = ",".join([f"\n\t{k} {v}" for k, v in cols.items()])
    schema_if_temp = f"[{table}]" if temp else f"[{schema}].[{table}]"
    return f"create table {schema_if_temp}\n({col_list}\n)"


def check_parameter_if_exists(if_exists: Literal["append", "fail", "replace"]):
    """Raises an error if parameter 'if_exists' is not correct"""
    if if_exists not in ("append", "fail", "replace"):
        raise InvalidParamError(
            f"Incorrect parameter value {if_exists} for 'if_exists'. Can be 'append', 'fail', or 'replace'"
        )

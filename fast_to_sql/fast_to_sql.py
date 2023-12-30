"""Main script that holds logic for fast_to_sql
"""
from __future__ import absolute_import

from typing import Any, Dict, Literal, Optional, Union

import pandas as pd
import pyodbc
from pandas import DataFrame, Series

from fast_to_sql.errors import FailError
from fast_to_sql.utils.utility_funs import (
    check_duplicate_cols,
    check_exists,
    check_parameter_if_exists,
    clean_col_name,
    clean_custom,
    clean_table_name,
    generate_create_statement,
    get_data_types,
    get_schema,
)


def fast_to_sql(
    df: Union[DataFrame, "Series[Any]"],
    name: str,
    conn: pyodbc.Connection,
    if_exists: Literal["append", "replace", "fail"] = "append",
    custom: Optional[Dict[str, str]] = None,
    temp: bool = False,
    copy: bool = False,
    clean_cols: bool = True,
):
    """Main fast_to_sql function.
    Writes pandas dataframe to sql using pyodbc fast_executemany
    """
    if copy:
        df = df.copy()

    # Assign null custom
    if custom is None:
        custom = {}

    # Handle series
    if isinstance(df, pd.Series):
        df = df.to_frame()

    # Clean table name
    name = clean_table_name(name, temp)

    # Clean columns
    columns = [clean_col_name(c) for c in list(df.columns)] if clean_cols else df.columns
    df.columns = columns

    # Check for duplicate column names
    check_duplicate_cols(df)
    custom = clean_custom(df, custom)

    # Assign data types
    data_types = get_data_types(df, custom)

    # Get schema
    cur = conn.cursor()
    schema, name = get_schema(cur, name)
    if schema == "":
        schema = cur.execute("SELECT SCHEMA_NAME()").fetchall()[0][0]
    exists = check_exists(cur, schema, name, temp)

    # Handle existing table
    create_statement = ""
    if exists:
        check_parameter_if_exists(if_exists)
        if if_exists == "replace":
            if temp:
                cur.execute(f"drop table [{name}]")
            else:
                cur.execute(f"drop table [{schema}].[{name}]")
            create_statement = generate_create_statement(schema, name, data_types, temp)
            cur.execute(create_statement)
        elif if_exists == "fail":
            fail_msg = (
                f"Temp table [{name}] already exists in this connection"
                if temp
                else f"Table [{schema}].[{name}] already exists"
            )
            raise FailError(fail_msg)
    else:
        create_statement = generate_create_statement(schema, name, data_types, temp)
        cur.execute(create_statement)

    # Run insert
    values_stmnt = f"({','.join(['?' for _ in data_types])})"
    if temp:
        insert_sql = f"insert into [{name}] values {values_stmnt}"
    else:
        insert_sql = f"insert into [{schema}].[{name}] values {values_stmnt}"
    insert_cols = df.values.tolist()
    insert_cols = [[None if pd.isna(cell) else cell for cell in row] for row in insert_cols]
    cur.fast_executemany = True
    cur.executemany(insert_sql, insert_cols)
    cur.close()
    return create_statement

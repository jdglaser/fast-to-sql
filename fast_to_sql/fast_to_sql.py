"""Main script that holds logic for fast_to_sql
"""
from __future__ import absolute_import
import pandas as pd 
import numpy as np
import pyodbc
from . import errors

# Global
DTYPE_MAP = {
    "int64": "int",
    "float64": "float",
    "object": "varchar(255)",
    "datetime64[ns]": "datetime2",
    "bool": "bit"
}

def _check_duplicate_cols(df):
    """Returns duplicate column names (case insensitive)
    """
    cols = [c.lower() for c in df.columns]
    dups = [x for x in cols if cols.count(x) > 1]
    if dups:
        raise errors.DuplicateColumns(f"There are duplicate column names. Repeated names are: {dups}. SQL Server dialect requires unique names (case insensitive).")

def _clean_col_name(column):
    """Removes special characters from column names
    """
    column = str(column).replace(" ", "_").replace("(","").replace(")","").replace("[","").replace("]","")
    column = f"[{column}]"
    return column

def _clean_custom(df, custom):
    """Validate and clean custom columns
    """
    for k in list(custom):
        clean_col = _clean_col_name(k)
        if clean_col not in df.columns:
            raise errors.CustomColumnException(f"Custom column {k} is not in the dataframe.")
        custom[clean_col] = custom.pop(k)
    return custom
    
def _get_data_types(df, custom):
    """Get data types for each column as dictionary
    Handles default data type assignment and custom data types
    """
    data_types = {}
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

def _get_default_schema(cur: pyodbc.Cursor) -> str:
    """Get the default schema of the caller
    """
    return str(cur.execute("select SCHEMA_NAME() as scm").fetchall()[0][0])

def _get_schema(cur: pyodbc.Cursor, table_name: str):
    """Get schema and table name - returned as tuple
    """
    t_spl = table_name.split(".")
    if len(t_spl) > 1:
        return t_spl[0], ".".join(t_spl[1:])
    else:
        return _get_default_schema(cur), table_name

def _clean_table_name(table_name):
    """Cleans the table name
    """
    return table_name.replace("'","''")

def _check_exists(cur,schema,table,temp):
    """Check in conn if table exists
    """
    if temp:
        return cur.execute(
            f"IF OBJECT_ID('tempdb..#[{table}]') IS NOT NULL select 1 else select 0"
        ).fetchall()[0][0]
    else:
        return cur.execute(
            f"IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}' and TABLE_SCHEMA = '{schema}') select 1 else select 0"
        ).fetchall()[0][0]

def _generate_create_statement(schema, table, cols, temp):
    """Generates a create statement
    """
    cols = ",".join([f'\n\t{k} {v}' for k, v in cols.items()])
    schema_if_temp = f"[#{table}]" if temp else f"[{schema}].[{table}]"
    return f"create table {schema_if_temp}\n({cols}\n)"

def _check_parameter_if_exists(if_exists):
    """Raises an error if parameter 'if_exists' is not correct
    """
    if if_exists not in ('append', 'fail', 'replace'):
        raise errors.WrongParam(f"Incorrect parameter value {if_exists} for 'if_exists'. Can be 'append', 'fail', or 'replace'")
        
def fast_to_sql(df, name, conn, if_exists='append', custom=None, temp=False, copy=False):
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
    name = _clean_table_name(name)

    # Clean columns
    columns = [_clean_col_name(c) for c in list(df.columns)]
    df.columns = columns

    # Check for duplicate column names 
    _check_duplicate_cols(df)
    custom = _clean_custom(df, custom)

    # Assign data types
    data_types = _get_data_types(df, custom)

    # Get schema
    cur = conn.cursor()
    schema, name = _get_schema(cur, name)
    if schema == '':
        schema = cur.execute("SELECT SCHEMA_NAME()").fetchall()[0][0]
    exists = _check_exists(cur, schema, name, temp)

    # Handle existing table
    create_statement = ''
    if exists:
        _check_parameter_if_exists(if_exists)
        if if_exists == "replace":
            cur.execute(f"drop table [{schema}].[{name}]")
            create_statement = _generate_create_statement(schema, name, data_types, temp)
            cur.execute(create_statement)
        elif if_exists == "fail":
            fail_msg = f"Table [{schema}].[{name}] already exists." if temp else f"Temp table #[{name}] already exists in this connection"
            raise errors.FailError(fail_msg)
    else:
        create_statement = _generate_create_statement(schema, name, data_types, temp)
        cur.execute(create_statement)

    # Run insert
    if temp:
        insert_sql = f"insert into [#{name}] values ({','.join(['?' for v in data_types])})"
    else:
        insert_sql = f"insert into [{schema}].[{name}] values ({','.join(['?' for v in data_types])})"
    insert_cols = df.values.tolist()
    insert_cols = [[None if type(cell) == float and np.isnan(cell) else cell for cell in row] for row in insert_cols]
    cur.fast_executemany = True
    cur.executemany(insert_sql, insert_cols)
    cur.close()
    return create_statement


        
        
        

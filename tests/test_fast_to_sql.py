import datetime
from typing import Any

import numpy
import pandas as pd
import pyodbc
import pytest
from pandas import Series

from fast_to_sql import fast_to_sql
from fast_to_sql.errors import CustomColumnException, DuplicateColumnsException, FailError
from fast_to_sql.utils.utility_funs import (
    check_duplicate_cols,
    check_exists,
    clean_col_name,
    clean_custom,
    clean_table_name,
    generate_create_statement,
    get_data_types,
    get_schema,
)

TEST_DF = pd.DataFrame(
    {"T1;'": [1, 2, 3], "[(Add)]": ["hello's", "My", "name"], "This is invalid": [True, False, False]}
)


@pytest.fixture(scope="session")
def conn():
    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=master;UID=sa;PWD=Passw@rd;", autocommit=True
    )
    conn.execute("CREATE DATABASE test")
    conn.close()
    conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=test;UID=sa;PWD=Passw@rd;")
    yield conn
    conn.close()
    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=master;UID=sa;PWD=Passw@rd;", autocommit=True
    )
    tables = ["test_table1", "test_table2", "test_table3", "test_table4", "test_table5", "testy1", "testy2"]
    for t in tables:
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    conn.execute("alter database test set single_user with rollback immediate")
    conn.execute("DROP DATABASE test")
    conn.commit()
    conn.close()


def test_clean_cols():
    clean_cols = [clean_col_name(c) for c in list(TEST_DF.columns)]
    assert ["[T1;']", "[Add]", "[This_is_invalid]"] == clean_cols


def test_dups():
    TEST_DF_NEW = TEST_DF.copy()
    TEST_DF_NEW["t1;'"] = 1

    def should_fail():
        check_duplicate_cols(TEST_DF_NEW)

    with pytest.raises(DuplicateColumnsException) as exc:
        should_fail()

    assert (
        str(exc.value) == 'There are duplicate column names. Repeated names are: ["t1;\'", "t1;\'"]. '
        "SQL Server dialect requires unique names (case insensitive)."
    )


def test_custom_dtype_error():
    TEST_DF_c = TEST_DF.copy()
    columns = [clean_col_name(c) for c in list(TEST_DF_c.columns)]
    TEST_DF_c.columns = columns

    with pytest.raises(CustomColumnException) as exc:
        clean_custom(TEST_DF_c, {"[(Add)]": "INT PRIMARY KEY", "fail": "DATE"})

    assert str(exc.value) == "Custom column fail is not in the dataframe."


def test_dtypes():
    TEST_DF_c = TEST_DF.copy()
    columns = [clean_col_name(c) for c in list(TEST_DF_c.columns)]
    TEST_DF_c.columns = columns
    custom = clean_custom(TEST_DF_c, {"[(Add)]": "INT PRIMARY KEY"})
    data_types = get_data_types(TEST_DF_c, custom)
    assert {"[T1;']": "bigint", "[Add]": "INT PRIMARY KEY", "[This_is_invalid]": "bit"} == data_types


def test_clean_name():
    table_name = "this isn't valid"
    assert "this isn''t valid" == clean_table_name(table_name)


def test_get_schema(conn: pyodbc.Connection):
    with conn.cursor() as cur:
        name = "dbo.test"
        schema, name = get_schema(cur, name)
        assert "dbo" == schema
        assert "test" == name

        name = "test"
        schema, name = get_schema(cur, name)
        assert "dbo" == schema
        assert "test" == name


def test_check_exists(conn: pyodbc.Connection):
    name = "dbo.test"
    cur = conn.cursor()
    schema, name = get_schema(cur, name)
    cur.execute(
        "IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'test' and TABLE_SCHEMA = SCHEMA_NAME()) "
        "DROP TABLE dbo.test"
    )
    cur.execute("create table dbo.test (t varchar(1))")
    res = check_exists(cur, schema, name, False)
    assert res is True
    cur.execute("drop table dbo.test")
    res = check_exists(cur, schema, name, False)
    assert res is False
    cur.close()


def test_generate_create_statement():
    df = pd.DataFrame({"A": [1, 2, 3], "B": ["a", "b", "c"], "C": [True, False, True]})
    data_types = get_data_types(df, {})
    create_statement = generate_create_statement("dbo", "test3", data_types, False)
    with open("tests/test_create.sql", "r") as f:
        expected = f.read()
    assert expected == create_statement


def test_big_numbers(conn: pyodbc.Connection):
    cur = conn.cursor()
    with open("tests/test_data.dat", "r") as f:
        data = f.read()
    data = data.split("\n")
    data = {i.split("|")[0]: [i.split("|")[1]] for i in data}
    data = pd.DataFrame(data)
    fast_to_sql(data, "testy1", conn, if_exists="replace", temp=False)
    conn.commit()

    test_val = int(cur.execute("select M from testy1").fetchall()[0][0])
    assert 352415214754234 == test_val


def test_null_values(conn: pyodbc.Connection):
    cur = conn.cursor()
    data = pd.read_csv("tests/test_data2.csv")
    fast_to_sql(data, "testy2", conn, if_exists="replace", temp=False)
    conn.commit()

    output = cur.execute("select * from testy2").fetchall()
    assert output[0][1] is None
    assert output[1][2] is None


def test_fast_to_sql(conn: pyodbc.Connection):
    """Test main fast_to_sql function"""
    cur = conn.cursor()

    # Simple test table
    df = pd.DataFrame({"A": [1, 2, 3], "B": ["a", "b", "c"], "C": [True, False, True]})
    fast_to_sql(df, "dbo.test_table2", conn, "replace", None, False)
    assert (1, "a", True) == tuple(cur.execute("select * from dbo.test_table2").fetchall()[0])

    # Series
    s: Series[Any] = pd.Series([1, 2, 3])
    fast_to_sql(s, "seriesTest", conn, "replace", None, False)
    assert 1 == cur.execute("select * from seriesTest").fetchall()[0][0]

    # Temp table
    df = pd.DataFrame({"A": [1, 2, 3], "B": ["a", "b", "c"], "C": [True, False, True]})
    fast_to_sql(s, "seriesTest", conn, "replace", None, True)
    assert 1 == cur.execute("select * from #seriesTest").fetchall()[0][0]

    # Custom Column Type
    df = pd.DataFrame({"A": ["1", "2", "3"], "B": ["a", "b", "c"], "C": [True, False, True]})
    fast_to_sql(df, "test_table3", conn, "replace", {"A": "INT PRIMARY KEY"}, False)
    with open("tests/get_col_def.sql", "r") as f:
        sql = f.read()
    res = cur.execute(sql).fetchall()
    assert "int" == res[0][1]
    assert res[0][6] is True

    # Fail if_exists
    with pytest.raises(FailError):
        fast_to_sql(df, "test_table3", conn, "fail", {"A": "INT PRIMARY KEY"}, False)

    # SQL output
    df = pd.DataFrame({"A": [4, 5, 6], "B": ["a", "b", "c"], "C": [True, False, True]})
    output = fast_to_sql(df, "test_table3", conn, "append", None, False)
    assert "" == output

    output = fast_to_sql(df, "test_table4", conn, "append", None, False)
    with open("tests/test_create_2.sql", "r") as f:
        expected = f.read()
    assert expected == output


def test_copy(conn: pyodbc.Connection):
    df2 = pd.DataFrame({"A Minus": [1, 2], "B Plus": [3, 4]})
    fast_to_sql(df2, "test_table4", conn, "replace", copy=True)
    assert df2.columns[0] == "A Minus"
    fast_to_sql(df2, "test_table4", conn, "replace")
    assert df2.columns[0] == "[A_Minus]"


def test_nan(conn: pyodbc.Connection):
    df3 = pd.DataFrame({"A": [1, numpy.NaN], "B": [numpy.nan, 4.3]})
    fast_to_sql(df3, "test_table5", conn, "replace")
    cur = conn.cursor()
    res = cur.execute("SELECT * FROM test_table5").fetchall()
    assert res[0][1] is None
    assert res[1][0] is None


def test_null_datetime(conn: pyodbc.Connection):
    df = pd.DataFrame({"A": [1, 2, 3], "B": [pd.Timestamp("20200101"), pd.Timestamp("20200202"), None]})
    fast_to_sql(df, "test_table5", conn, "replace")
    cur = conn.cursor()
    res = cur.execute("SELECT * FROM test_table5").fetchall()
    assert datetime.datetime(2020, 1, 1, 0, 0) == res[0][1]


def test_no_clean_col_names(conn: pyodbc.Connection):
    df = pd.DataFrame({"[My Special Col]": [1, 2, 3], "BCol": [4, 5, 6]})
    sql = fast_to_sql(df, "test_table6", conn, "replace", clean_cols=False)
    assert """create table [dbo].[test_table6]\n(\n\t[My Special Col] bigint,\n\tBCol bigint\n)""" == sql
    cur = conn.cursor()
    cur.execute("SELECT * FROM test_table6")
    assert ["My Special Col", "BCol"] == [column[0] for column in cur.description]

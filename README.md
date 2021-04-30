# fast_to_sql

## Introduction

`fast_to_sql` is an improved way to upload pandas dataframes to Microsoft SQL Server.

`fast_to_sql` takes advantage of pyodbc rather than SQLAlchemy. This allows for a much lighter weight import for writing pandas dataframes to sql server. It uses pyodbc's `executemany` method with `fast_executemany` set to `True`, resulting in far superior run times when inserting data. 

## Installation

```python
pip install fast_to_sql
```

## Requirements

* Written for Python 3.8+
* Requires pandas, pyodbc

## Example

```py
from datetime import datetime

import pandas as pd

import pyodbc
from fast_to_sql import fast_to_sql as fts

# Test Dataframe for insertion
df = pd.DataFrame({
    "Col1": [1, 2, 3],
    "Col2": ["A", "B", "C"],
    "Col3": [True, False, True],
    "Col4": [datetime(2020,1,1),datetime(2020,1,2),datetime(2020,1,3)]
})

# Create a pyodbc connection
conn = pyodbc.connect(
    """
    Driver={ODBC Driver 17 for SQL Server};
    Server=localhost;
    Database=my_database;
    UID=my_user;
    PWD=my_pass;
    """
)

# If a table is created, the generated sql is returned
create_statement = fts.fast_to_sql(df, "my_great_table", conn, if_exists="replace", custom={"Col1":"INT PRIMARY KEY"}, temp=False)

# Commit upload actions and close connection
conn.commit()
conn.close()
```

## Usage

### Main function

```python
fts.fast_to_sql(df, name, conn, if_exists="append", custom=None, temp=False, copy=False)
```

* ```df```: pandas DataFrame to upload
* ```name```: String of desired name for the table in SQL server
* ```conn```: A valid pyodbc connection object
* ```if_exists```: Option for what to do if the specified table name already exists in the database. If the table does not exist a new one will be created. By default this option is set to 'append'
  * __'append'__: Appends the dataframe to the table if it already exists in SQL server.
  * __'fail'__: Purposely raises a `FailError` if the table already exists in SQL server.
  * __'replace'__: Drops the old table with the specified name, and creates a new one. **Be careful with this option**, it will completely delete a table with the specified name in SQL server.
* ```custom```: A dictionary object with one or more of the column names being uploaded as the key, and a valid SQL column definition as the value. The value must contain a type (`INT`, `FLOAT`, `VARCHAR(500)`, etc.), and can optionally also include constraints (`NOT NULL`, `PRIMARY KEY`, etc.)
  * Examples: 
  `{'ColumnName':'varchar(1000)'}` 
  `{'ColumnName2':'int primary key'}`
* ```temp```: Either `True` if creating a local sql server temporary table for the connection, or `False` (default) if not.
* ```copy```: Defaults to `False`. If set to `True`, a copy of the dataframe will be made so column names of the original dataframe are not altered. Use this if you plan to continue to use the dataframe in your script after running `fast_to_sql`.












# fast_to_sql

## Introduction

fast_to_SQL is an improved way to upload pandas dataframes to MS SQL server. The method borrows an idea from [here](https://iabdb.me/2016/07/13/a-better-way-load-data-into-microsoft-sql-server-from-pandas/), and turns it into a usable function. This function takes advantage of MS SQL server's multi-row insert ability. This can lead to MUCH faster speeds for uploading dataframes to SQL server (uploading a 10,000 row 5 column dataframe with pd.to_sql() took 517.97s, while uploading the same dataframe with fast_to_SQL took only 5.45s!). 

The funciton also automatically preserves datatypes for: integer, float, string, boolean, and datetime64[ns] and converts them to SQL datatypes: int, float, varchar(255), bit, and datetime. Custom data types can also be set for a subset or all of the columns (see [usage](#usage)).

## Installation

```python
pip install fast_to_sql
```

## Requirements

* Written for Python 3.6+
* Requires pandas, sqlalchemy, datetime

## Example

```python
from fast_to_SQL import fast_to_sql as fts

# Create a SQL Alchemy Engine to desired server/database
sqluser = "DOMAIN\USER"
sqlpass = "Password"
server = "some_server"
db = "some_DB"

engine = create_engine("mssql+pyodbc://{}:{}@{}/{}?driver=SQL+Server&trusted_connection=true"
                       .format(sqluser,sqlpass,server,db))

df = SomePandasDF

# Run main function
fts.to_sql_fast(df, 'DFName', engine, if_exists = 'append', series = False, custom = {'column1':varchar(500)}, temp = False)
```

## Usage

### Main function

```python
fts.to_sql_fast(df, name, engine, if_exists = 'append', series = False, custom = None, temp = False)
```

* ```df```: pandas DataFrame to upload
* ```name```: String of desired name for the table in SQL server
* ```engine```: A SQL alchemy engine
* ```if_exists```: Option for what to do if the specified name already exists in the dataframe. If the dataframe does not exist a new one will be created. By default this option is set to 'append'
  * __'append'__: Appends the dataframe to the table if it already exists in SQL server.
  * __'fail'__: Purposely raises a FailError if the table already exists in SQL server.
  * __'replace'__: Drops the old table with the specified name, and creates a new one. Be careful with this option, it will completely delete a table with the specified name in SQL server.
* ```series```: By default this is set to False. Set to True if the DataFrame is a series (only has one column).
* ```custom```: A dictionary object with one or more of the column names being uploaded as the key, and a valid SQL data type as the value, this will override the default data type assigned to the column by the function.
  * Example: `{'ColumnName':'varchar(1000)'}`
* ```temp```: Either `True` if creating a local temporary table, or `False` (default) if not. If set to `True` the temporary table will be dropped after the connection is closed

## Caveats

* This has only been tested with Microsoft SQL Server 2016 and `pyodbc` This may not work for other SQL databases.
* The larger the database, the smaller speed imrpovements you will most likely see. This means that a 100 column, 500,000 row table, may still take a while to upload. This is because multi-row insert can only do a max of 1000 rows at a time.

## Credits

* This package is based on an excellent article from [here](https://iabdb.me/2016/07/13/a-better-way-load-data-into-microsoft-sql-server-from-pandas/)








# fast_to_SQL

## Introduction

fast_to_SQL is an improved way to upload pandas dataframes to MS SQL server. The method borrows an idea from [here](https://iabdb.me/2016/07/13/a-better-way-load-data-into-microsoft-sql-server-from-pandas/), and turns it into a usable function. This function takes advantage of MS SQL server's multi-row insert ability. This can lead to MUCH faster speeds for uploading dataframes to SQL server (uploading a 10,000 row 5 column dataframe with pd.to_sql() took 517.97s, while uploading the same dataframe with fast_to_SQL took took about only 5.45s!). 

The funciton also automatically preserves datatypes for: integer, float, string, boolean, and datetime64[ns] and converts them to SQL datatypes: int, float, varchar(255), bit, and datetime.

## Installation

This module is not on pip. To install just download the fast_to_sql script to the same directory as your project, and import as seen above.

## Requirements

* Written for Python 3.6+
* Requires pandas, sqlalchemy, datetime.datetime

## Example

```python
import fast_to_SQL as fts

# Create a SQL Alchemy Engine to desired server/database
sqluser = "DOMAIN\USER"
sqlpass = "Password"
server = "some_server"
db = "some_DB"

engine = create_engine("mssql+pyodbc://{}:{}@{}/{}?driver=SQL+Server&trusted_connection=true"
                       .format(sqluser,sqlpass,server,db))

df = SomePandasDF

# Run main function
fts.to_sql_fast(df, 'DFName', engine, if_exists = 'append', series = False)
```

## Usage








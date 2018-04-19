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

### Main function

```python
fts.to_sql_fast(df, name, engine, if_exists = 'append', series = False)
```

* ```df```: pandas DataFrame to upload
* ```name```: String of desired name for the table in SQL server
* ```engine```: A SQL alchemy engine
* ```if_exists```: Option for what to do if the specified name already exists in the dataframe. If the dataframe does not exist a new one will be created. By default this option is set to 'append'
  * __'append'__: Appends the dataframe to the table if it already exists in SQL server.
  * __'fail'__: Purposely raises a FailError if the table already exists in SQL server.
  * __'replace'__: Drops the old table with the specified name, and creates a new one. Be careful with this option, it will completely delete a table with the specified name in SQL server.
* ```series```: By default this is set to False. Set to True if the DataFrame is a series (only has one column).

## Caveats

* This has only been tested with Microsoft SQL Server 2008. This may not work for other SQL databases.
* The larger the database, the smaller speed imrpovements you will most likely see. This means that a 100 column, 500,000 row table, may still take a while to upload. This is because multi-row insert can only do a max of 1000 rows at a time.
* Some of the try and except statements in the script that check for existsing tables, use just a general except statement rather than specifying an error. While this is generally bad practice, the errors returned were specific to Pyodbc, and I wanted to keep the funciton open to other Python + SQL methods as well. If anyone is willing to test/improve this function for uses outside of mssql+pyodbc, please do!

## Credits

* An excellent article [here](https://iabdb.me/2016/07/13/a-better-way-load-data-into-microsoft-sql-server-from-pandas/) was what inspired me to create this function.








# fast_to_SQL

fast_to_SQL is an improved way to upload pandas dataframes to MS SQL server. The method borrows an idea from [here](https://iabdb.me/2016/07/13/a-better-way-load-data-into-microsoft-sql-server-from-pandas/) and turns it into a usable function. This function takes advantage of MS SQL server's multi-row insert ability. This can lead to MUCH faster speeds for uploading dataframes to SQL server (uploading a 10,000 row 5 column dataframe with fast_to_SQL took about 5.45s, while pd.to_sql() on the same dataframe took 517.97s!). 

The funciton also automatically preserves datatypes for: integer, float, string, boolean, and datetime64[ns] and converts them to SQL datatypes: int, float, varchar(255), bit, and datetime.

## Example

```python
# Create a SQL Alchemy Engine to desired server/database
engine = SomeSQLalchemyEngine

df = SomePandasDF

# Run main function
fts.to_sql_fast(df, 'DFName', engine, if_exists = 'append', series = False)
```




import pandas as pd
from sqlalchemy import create_engine, exc
from datetime import datetime

# Define Errors
class FailError(Exception):
    pass
class WrongParam(Exception):
    pass
class InvalidEngine(Exception):
    pass
class DuplicateColumns(Exception):
    pass

# Define funciton for chunk splitting
def chunk_split(seq, size):
    return (seq[pos:pos + size] for pos in range(0,len(seq),size))

# Main function      
def to_sql_fast(df,name,engine,if_exists='append',series=False):
    # Copy DF to avoid changing instance elsewhere
    df = df.copy()
    # Replace all commas in dataframe to avoid SQl error
    object_cols = list(df.select_dtypes(include='object').columns)
    df[object_cols] = df[object_cols].apply(lambda x: x.str.replace('"',""))
    df[object_cols] = df[object_cols].apply(lambda x: x.str.replace("'",""))

    # Check for valid engine
    try:
        engine.connect()
    except:
        raise InvalidEngine("Cannot connect to sqlalchemy engine.")
    
    # Check for duplicate column names
    lower_cols = [c.lower() for c in df.columns]
    check = list(set([x for x in lower_cols if lower_cols.count(x) > 1]))

    if check:
        raise DuplicateColumns("There are duplicate column names. Columns named twice are: {}. SQL must have unique names (case insensitive)".format(check))

    if series == True:
        df = df.to_frame()
    
    cols = df.columns.tolist()
    
    # Get rid of invalid character in title
    for i in range(len(cols)):
        cols[i] = cols[i].replace(" ","_").replace("(","").replace(")","")
        cols[i] = "[" + cols[i] + "]"
    df.columns = cols
    
    # Create list of columns and their datatypes
    cols = [c for c in df.columns]
    dtypes = [str(i) for i in df.dtypes]
    sql_dtypes = []
    for i in range(len(dtypes)):
        if dtypes[i] == 'int64':
            df[cols[i]] = df[cols[i]].fillna(0)
            sql_dtypes.append("int")
        elif dtypes[i] == 'float64':
            df[cols[i]] = df[cols[i]].fillna(0)
            sql_dtypes.append("float")
        elif dtypes[i] == 'object':
            df[cols[i]] = df[cols[i]].fillna("NULL")
            sql_dtypes.append("varchar(255)")
        elif dtypes[i] == 'datetime64[ns]':
            df[cols[i]] = df[cols[i]].astype(str)
            df[cols[i]] = df[cols[i]].fillna("NULL")
            sql_dtypes.append("datetime")
        elif dtypes[i] == 'bool':
            df.loc[df[cols[i]] == True, [cols[i]]] = 1
            df.loc[df[cols[i]] == False, [cols[i]]] = 0
            df[cols[i]] = df[cols[i]].fillna("NULL")
            sql_dtypes.append("bit")
        else:
            df[cols[i]] = df[cols[i]].fillna("NULL")
            sql_dtypes.append("varchar(255)")
    
    col_string_create = "(" + ', '.join([cols[i] + " " + sql_dtypes[i] for i in range(len(cols))]) + ")"
    col_string_insert = "(" + ', '.join(cols) + ")"
    # Define records for inserting
    records = [str(tuple(x)) for x in df.values]
    
    # Restructure records if a series
    if series == True:
        records = ["('" + str(x[0]) + "')" for x in df.values]
    
    # Define insert string
    insert = "INSERT INTO {} {} VALUES ".format(name,col_string_insert)
     
    # Define the function that will insert rows
    def batch_run():
        for batch in chunk_split(records, 1000):
            rows = ','.join(batch)
            insert_batch = insert + "{}".format(rows)
            insert_batch = insert_batch.replace("'NULL'","NULL")
            engine.execute(insert_batch)
    
    # Check if table exists
    try:
        engine.execute(("CREATE TABLE {}"
                        " {};").format(name, col_string_create))
        exists = False
    except exc.SQLAlchemyError as e:
        if "there is already an object named" in str(e).lower():
            exists = True
        else:
            raise e
   
    # Decide what to do based on whether the table exists
    # If table does exist
    if exists == True:
        if if_exists == 'append':
            batch_run()
        elif if_exists == 'fail':
            raise FailError("The table already exists. Set 'if_exists' to 'append' if you want to add to it, or 'replace' if you want to replace it.") 
        # BE CAREFUL WITH THIS OPTION - WILL DELETE AND REPLACE TABLE
        elif if_exists == 'replace':
            engine.execute("DROP TABLE {}".format(name))
            engine.execute(("CREATE TABLE {}"
                            " {};").format(name, col_string_create))
            batch_run()
        else:
            raise WrongParam("Incorrect Parameter used for 'if_exists'. Can be 'append', 'fail', or 'replace'.")
    # If table doesn't exist
    else:
        batch_run()

    
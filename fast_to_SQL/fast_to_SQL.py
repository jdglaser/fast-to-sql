import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Define Errors
class FailError(Exception):
    pass
class WrongParam(Exception):
    pass
class InvalidEngine(Exception):
    pass

# Define funciton for chunk splitting
def chunk_split(seq, size):
    return (seq[pos:pos + size] for pos in range(0,len(seq),size))

# Main function      
def to_sql_fast(df,name,engine,if_exists='append',series=False):
    # Check for valid engine
    try:
        engine.connect()
    except:
        raise InvalidEngine("Cannot connect to sqlalchemy engine.")
    
    if series == True:
        df = df.to_frame()
    
    cols = df.columns.tolist()
    
    # Get rid of invalid character in title
    for i in range(len(cols)):
        cols[i] = cols[i].replace(" ","_").replace("(","").replace(")","")
    df.columns = cols
    
    # Create list of columns and their datatypes
    cols = [c for c in df.columns]
    dtypes = [str(i) for i in df.dtypes]
    print(dtypes)
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
    print(df)
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
            engine.execute(insert_batch)
    
    # Check if table exists
    try:
        engine.execute(("CREATE TABLE {}"
                        " {};").format(name, col_string_create))
        exists = False
    except:
        exists = True
   
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



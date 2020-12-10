import fast_to_sql.fast_to_sql as fts
from fast_to_sql import errors
import pandas as pd  
import unittest
import pyodbc

# Intentionally included weird column names
TEST_DF = pd.DataFrame({
    "T1;'": [1,2,3],
    "[(Add)]": ["hello's","My","name"],
    "This is invalid": [True, False, False]
})

conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=localhost\SQLEXPRESS;Database=test;Trusted_Connection=yes;")


# Tests
class FastToSQLTests(unittest.TestCase):

    def test_clean_cols(self):
        clean_cols = [fts._clean_col_name(c) for c in list(TEST_DF.columns)]
        self.assertEqual(["[T1;']", '[Add]', '[This_is_invalid]'],clean_cols)

    def test_dups(self):
        TEST_DF_NEW = TEST_DF.copy()
        TEST_DF_NEW["t1;'"] = 1
        def should_fail():
            fts._check_duplicate_cols(TEST_DF_NEW)
        self.assertRaises(errors.DuplicateColumns,should_fail)
    
    def test_custom_dtype_error(self):
        TEST_DF_c = TEST_DF.copy()
        columns = [fts._clean_col_name(c) for c in list(TEST_DF_c.columns)]
        TEST_DF_c.columns = columns
        def should_fail():
            fts._clean_custom(TEST_DF_c, {"[(Add)]":"INT PRIMARY KEY","fail":"DATE"})
        self.assertRaises(errors.CustomColumnException,should_fail)

    def test_dtypes(self):
        TEST_DF_c = TEST_DF.copy()
        columns = [fts._clean_col_name(c) for c in list(TEST_DF_c.columns)]
        TEST_DF_c.columns = columns
        custom = fts._clean_custom(TEST_DF_c, {"[(Add)]":"INT PRIMARY KEY"})
        data_types = fts._get_data_types(TEST_DF_c, custom)
        self.assertEqual({"[T1;']": 'int', '[Add]': 'INT PRIMARY KEY', '[This_is_invalid]': 'bit'},data_types)

    def test_clean_name(self):
        table_name = "this isn't valid"
        self.assertEqual("this isn''t valid",fts._clean_table_name(table_name))
    
    def test_get_schema(self):
        name = "dbo.test"
        schema, name = fts._get_schema(name)
        self.assertEqual("dbo",schema)
        self.assertEqual("test",name)
    
    def test_check_exists(self):
        name = "dbo.test"
        schema, name = fts._get_schema(name)
        cur = conn.cursor()
        cur.execute("IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'test' and TABLE_SCHEMA = SCHEMA_NAME()) DROP TABLE dbo.test")
        cur.execute("create table dbo.test (t varchar(1))")
        res = fts._check_exists(cur, schema, name, False)
        self.assertEqual(1, res)
        cur.execute("drop table dbo.test")
        res = fts._check_exists(cur, schema, name, False)
        self.assertEqual(0, res)
        cur.close()
    
    def test_check_parameter_if_exists(self):
        def should_fail():
            fts._check_parameter_if_exists("should_fail")
        self.assertRaises(errors.WrongParam,should_fail)

    def test_generate_create_statement(self):
        df = pd.DataFrame({"A":[1,2,3],"B":["a","b","c"],"C":[True,False,True]})
        data_types = fts._get_data_types(df, {})
        create_statement = fts._generate_create_statement("dbo","test3",data_types,"")
        with open("tests/test_create.sql","r") as f:
            compare = f.read()
        self.assertEqual(compare, create_statement)

    def test_big_numbers(self):
        with open("tests/test_data.dat", "r") as f:
            data = f.read()
        data = data.split("\n")
        data = {i.split("|")[0]: [i.split("|")[1]] for i in data}
        data = pd.DataFrame(data)
        fts.fast_to_sql(data, "test1", conn, if_exists="replace", temp=False)
        conn.commit()
    
    def test_fast_to_sql(self):
        """Test main fast_to_sql function
        """
        cur = conn.cursor()

        # Simple test table
        df = pd.DataFrame({"A":[1,2,3],"B":["a","b","c"],"C":[True,False,True]})
        fts.fast_to_sql(df, "dbo.test_table2", conn, "replace", None, False)
        self.assertEqual((1, 'a', True),tuple(cur.execute("select * from dbo.test_table2").fetchall()[0]))

        # Series
        s = pd.Series([1,2,3])
        fts.fast_to_sql(s, "seriesTest", conn, "replace", None, False)
        self.assertEqual(1,cur.execute("select * from seriesTest").fetchall()[0][0])

        # Temp table
        df = pd.DataFrame({"A":[1,2,3],"B":["a","b","c"],"C":[True,False,True]})
        fts.fast_to_sql(s, "seriesTest", conn, "replace", None, True)
        self.assertEqual(1,cur.execute("select * from #seriesTest").fetchall()[0][0])

        # Custom Column Type
        df = pd.DataFrame({"A":["1","2","3"],"B":["a","b","c"],"C":[True,False,True]})
        fts.fast_to_sql(df, "test_table3", conn, "replace", {"A":"INT PRIMARY KEY"}, False)
        with open("tests/get_col_def.sql","r") as f:
            sql = f.read()
        res = cur.execute(sql).fetchall()
        self.assertEqual("int",res[0][1])
        self.assertTrue(res[0][6])

        # Fail if_exists
        def should_fail():
            fts.fast_to_sql(df, "test_table3", conn, "fail", {"A":"INT PRIMARY KEY"}, False)
        self.assertRaises(errors.FailError,should_fail)

        # SQL output
        df = pd.DataFrame({"A":[4, 5, 6],"B":["a","b","c"],"C":[True,False,True]})
        output = fts.fast_to_sql(df, "test_table3", conn, "append", None, False)
        self.assertEqual("",output)

        output = fts.fast_to_sql(df, "test_table4", conn, "append", None, False)
        with open("tests/test_create_2.sql","r") as f:
            compare = f.read()
        self.assertEqual(compare, output)

if __name__ == '__main__':
    unittest.main()
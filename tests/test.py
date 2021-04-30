import fast_to_sql.fast_to_sql as fts
from fast_to_sql import errors
import datetime
import pandas as pd  
import unittest
import pyodbc
import numpy as np

# Tests
class FastToSQLTests(unittest.TestCase):
    
    conn = None

    # Intentionally included weird column names
    TEST_DF = pd.DataFrame({
        "T1;'": [1,2,3],
        "[(Add)]": ["hello's","My","name"],
        "This is invalid": [True, False, False]
    })

    @classmethod
    def setUpClass(cls):
        cls.conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=test;UID=sa;PWD=Pass@word;")

    @classmethod
    def tearDown(self):
        tables = ["test_table1","test_table2","test_table3","test_table4","test_table5","testy1","testy2"]
        for t in tables:
            self.conn.execute(f"DROP TABLE IF EXISTS {t}")
        self.conn.commit()

    def test_clean_cols(self):
        clean_cols = [fts._clean_col_name(c) for c in list(self.TEST_DF.columns)]
        self.assertEqual(["[T1;']", '[Add]', '[This_is_invalid]'],clean_cols)

    def test_dups(self):
        TEST_DF_NEW = self.TEST_DF.copy()
        TEST_DF_NEW["t1;'"] = 1
        def should_fail():
            fts._check_duplicate_cols(TEST_DF_NEW)
        self.assertRaises(errors.DuplicateColumns,should_fail)
    
    def test_custom_dtype_error(self):
        TEST_DF_c = self.TEST_DF.copy()
        columns = [fts._clean_col_name(c) for c in list(TEST_DF_c.columns)]
        TEST_DF_c.columns = columns
        def should_fail():
            fts._clean_custom(TEST_DF_c, {"[(Add)]":"INT PRIMARY KEY","fail":"DATE"})
        self.assertRaises(errors.CustomColumnException,should_fail)

    def test_dtypes(self):
        TEST_DF_c = self.TEST_DF.copy()
        columns = [fts._clean_col_name(c) for c in list(TEST_DF_c.columns)]
        TEST_DF_c.columns = columns
        custom = fts._clean_custom(TEST_DF_c, {"[(Add)]":"INT PRIMARY KEY"})
        data_types = fts._get_data_types(TEST_DF_c, custom)
        self.assertEqual({"[T1;']": 'int', '[Add]': 'INT PRIMARY KEY', '[This_is_invalid]': 'bit'},data_types)

    def test_clean_name(self):
        table_name = "this isn't valid"
        self.assertEqual("this isn''t valid",fts._clean_table_name(table_name))
    
    def test_get_schema(self):
        cur = self.conn.cursor()

        name = "dbo.test"
        schema, name = fts._get_schema(cur, name)
        self.assertEqual("dbo",schema)
        self.assertEqual("test",name)
        
        name = "test"
        schema, name = fts._get_schema(cur, name)
        self.assertEqual("dbo", schema)
        self.assertEqual("test", name)
    
    def test_check_exists(self):
        name = "dbo.test"
        cur = self.conn.cursor()
        schema, name = fts._get_schema(cur, name)
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
        cur = self.conn.cursor()
        with open("tests/test_data.dat", "r") as f:
            data = f.read()
        data = data.split("\n")
        data = {i.split("|")[0]: [i.split("|")[1]] for i in data}
        data = pd.DataFrame(data)
        fts.fast_to_sql(data, "testy1", self.conn, if_exists="replace", temp=False)
        self.conn.commit()

        test_val = int(cur.execute("select M from testy1").fetchall()[0][0])
        self.assertEqual(352415214754234,test_val)

    def test_null_values(self):
        cur = self.conn.cursor()
        data = pd.read_csv("tests/test_data2.csv")
        fts.fast_to_sql(data, "testy2", self.conn, if_exists="replace", temp=False)
        self.conn.commit()

        output = cur.execute("select * from testy2").fetchall()
        self.assertIsNone(output[0][1])
        self.assertIsNone(output[1][2])
    
    def test_fast_to_sql(self):
        """Test main fast_to_sql function
        """
        cur = self.conn.cursor()

        # Simple test table
        df = pd.DataFrame({"A":[1,2,3],"B":["a","b","c"],"C":[True,False,True]})
        fts.fast_to_sql(df, "dbo.test_table2", self.conn, "replace", None, False)
        self.assertEqual((1, 'a', True),tuple(cur.execute("select * from dbo.test_table2").fetchall()[0]))

        # Series
        s = pd.Series([1,2,3])
        fts.fast_to_sql(s, "seriesTest", self.conn, "replace", None, False)
        self.assertEqual(1,cur.execute("select * from seriesTest").fetchall()[0][0])

        # Temp table
        df = pd.DataFrame({"A":[1,2,3],"B":["a","b","c"],"C":[True,False,True]})
        fts.fast_to_sql(s, "seriesTest", self.conn, "replace", None, True)
        self.assertEqual(1,cur.execute("select * from #seriesTest").fetchall()[0][0])

        # Custom Column Type
        df = pd.DataFrame({"A":["1","2","3"],"B":["a","b","c"],"C":[True,False,True]})
        fts.fast_to_sql(df, "test_table3", self.conn, "replace", {"A":"INT PRIMARY KEY"}, False)
        with open("tests/get_col_def.sql","r") as f:
            sql = f.read()
        res = cur.execute(sql).fetchall()
        self.assertEqual("int",res[0][1])
        self.assertTrue(res[0][6])

        # Fail if_exists
        def should_fail():
            fts.fast_to_sql(df, "test_table3", self.conn, "fail", {"A":"INT PRIMARY KEY"}, False)
        self.assertRaises(errors.FailError,should_fail)

        # SQL output
        df = pd.DataFrame({"A":[4, 5, 6],"B":["a","b","c"],"C":[True,False,True]})
        output = fts.fast_to_sql(df, "test_table3", self.conn, "append", None, False)
        self.assertEqual("",output)

        output = fts.fast_to_sql(df, "test_table4", self.conn, "append", None, False)
        with open("tests/test_create_2.sql","r") as f:
            compare = f.read()
        self.assertEqual(compare, output)

    def test_copy(self):
        df2 = pd.DataFrame({"A Minus":[1,2],"B Plus":[3,4]})
        fts.fast_to_sql(df2, "test_table4", self.conn, "replace", copy=True)
        self.assertEqual(df2.columns[0], "A Minus")
        fts.fast_to_sql(df2, "test_table4", self.conn, "replace")
        self.assertEqual(df2.columns[0], "[A_Minus]")

    def test_nan(self):
        df3 = pd.DataFrame({"A": [1, np.NaN], "B": [np.nan, 4.3]})
        fts.fast_to_sql(df3, "test_table5", self.conn, "replace")
        cur = self.conn.cursor()
        res = cur.execute("SELECT * FROM test_table5").fetchall()
        self.assertIsNone(res[0][1])
        self.assertIsNone(res[1][0])

    def test_null_datetime(self):
        df = pd.DataFrame({
            "A": [1,2,3], 
            "B": [pd.Timestamp("20200101"), pd.Timestamp("20200202"), None]})
        fts.fast_to_sql(df, "test_table5", self.conn, "replace")
        cur = self.conn.cursor()
        res = cur.execute("SELECT * FROM test_table5").fetchall()
        self.assertEqual(datetime.datetime(1,1,1,0,0), res[2][1])

if __name__ == '__main__':
    unittest.main()
from pytest import raises


from ezduckdb import DuckDB, SQL
import pandas as pd


class TestSQL:
    def test_normal_bindings(self):
        inp = SQL("SELECT * FROM $table WHERE id = $id", table="foo", id=1)
        assert inp.to_string() == "SELECT * FROM 'foo' WHERE id = 1"

    def test_nonexistant_bindings(self):
        with raises(AssertionError) as _:
            SQL("SELECT * FROM table", id=1)

    def test_df_bindings(self):
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3]})
        inp = SQL("SELECT * FROM $table", table=df)
        assert inp.to_string() == "SELECT * FROM df_" + str(id(df))

    def test_sql_bindings(self):
        inp = SQL("SELECT * FROM $table", table=SQL("SELECT * FROM foo"))
        assert inp.to_string() == "SELECT * FROM (SELECT * FROM foo)"

    def test_recursive_sql_bindings(self):
        inp = SQL(
            "SELECT * FROM $table", table=SQL("SELECT * FROM $table", table="foo")
        )
        assert inp.to_string() == "SELECT * FROM (SELECT * FROM 'foo')"


class TestDuckDB:
    def test_query(self):
        db = DuckDB()
        assert db.query(SQL("select 1")).values == pd.DataFrame([(1,)]).values

    def test_context_manager(self):
        with DuckDB() as conn:
            assert conn.query("select 1").df().values == pd.DataFrame([(1,)]).values

    def test_df_bindings(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        db = DuckDB()
        act = db.query(SQL("SELECT * FROM $table", table=df)).values
        exp = pd.DataFrame([(1,), (2,), (3,)]).values
        assert (act == exp).all()

    def test_sql_bindings(self):
        db = DuckDB()
        act = db.query(
            SQL("SELECT * FROM $table", table=SQL("SELECT $val", val=1))
        ).values
        exp = pd.DataFrame([(1,)]).values
        assert (act == exp).all()

    def test_query_s3_parquet(self):
        s3_path = (
            "s3://codenym-automated-testing/ezduckdb/parquet/schema1_table1.parquet"
        )
        db = DuckDB(aws_profile="codenym")
        act = db.query(
            SQL("SELECT * FROM read_parquet($s3_path)", s3_path=s3_path)
        ).values
        exp = pd.DataFrame([[1, 4], [2, 5], [3, 6]]).values

        assert (act == exp).all()

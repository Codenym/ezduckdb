from ezduckdb import FlexPath, get_table_name_from_path
import pytest


class TestFlexPath:
    def test_one(self):
        inp = FlexPath("s3://bucket/curated/s1chema_table1.csv")
        assert inp.get_s3_bucket() == "bucket"
        assert inp.get_s3_prefix() == "curated/s1chema_table1.csv"
        assert str(inp) == "s3://bucket/curated/s1chema_table1.csv"
        assert inp.is_s3() == True

    def test_two(self):
        inp = FlexPath("curated/s1chema_table1.csv")
        with pytest.raises(Exception) as _:
            inp.get_s3_bucket()
        with pytest.raises(Exception) as _:
            assert inp.get_s3_prefix()
        assert str(inp) == "curated/s1chema_table1.csv"
        assert inp.is_s3() == False


class Test_get_table_name_from_path:
    def test_one(self):
        inp = FlexPath("curated/s1chema_table1.csv")
        assert get_table_name_from_path(inp) == ("s1chema", "table1")

    def test_two(self):
        inp = FlexPath("curated/s1chema_table1_blah.parquet")
        assert get_table_name_from_path(inp) == ("s1chema", "table1_blah")

    def test_three(self):
        inp = FlexPath("s3://bucket/curated/s1chema_table2_blah.parquet")
        assert get_table_name_from_path(inp) == ("s1chema", "table2_blah")

from ezduckdb import S3AwarePath
import pytest


class TestS3AwarePath:
    def test_one(self):
        inp = S3AwarePath("s3://bucket/curated/s1chema_table1.csv")
        assert inp.get_s3_bucket() == "bucket"
        assert inp.get_s3_prefix() == "curated/s1chema_table1.csv"
        assert str(inp) == "s3://bucket/curated/s1chema_table1.csv"
        assert inp.is_s3()

    def test_two(self):
        inp = S3AwarePath("curated/s1chema_table1.csv")
        with pytest.raises(Exception) as _:
            inp.get_s3_bucket()
        with pytest.raises(Exception) as _:
            assert inp.get_s3_prefix()
        assert str(inp) == "curated/s1chema_table1.csv"
        assert not inp.is_s3()

    def test_get_table_name_local_csv(self):
        inp = S3AwarePath("curated/s1chema_table1.csv")
        assert inp.get_table_name() == ("s1chema", "table1")

    def test_get_table_name_local_parquet(self):
        inp = S3AwarePath("curated/s1chema_table1_blah.parquet")
        assert inp.get_table_name() == ("s1chema", "table1_blah")

    def test_get_table_name_s3_parquet(self):
        inp = S3AwarePath("s3://bucket/curated/s1chema_table2_blah.parquet")
        assert inp.get_table_name() == ("s1chema", "table2_blah")

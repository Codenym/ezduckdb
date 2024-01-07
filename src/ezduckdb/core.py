from duckdb import connect
import pandas as pd
from sqlescapy import sqlescape
from string import Template
from typing import Mapping
from datanym import S3AwarePath


class SQL:
    def __init__(self, sql, **bindings):
        for binding in bindings:
            assert binding in sql
        self.sql = sql
        self.bindings = bindings

    def to_string(self) -> str:
        replacements = {}
        for key, value in self.bindings.items():
            if isinstance(value, pd.DataFrame):
                replacements[key] = f"df_{id(value)}"
            elif isinstance(value, SQL):
                replacements[key] = f"({value.to_string()})"
            elif isinstance(value, (str, S3AwarePath)):
                replacements[key] = f"'{sqlescape(value)}'"
            elif isinstance(value, (int, float, bool)):
                replacements[key] = str(value)
            elif value is None:
                replacements[key] = "null"
            else:
                raise ValueError(f"Invalid type for {key}")
        return Template(self.sql).safe_substitute(replacements)

    def collect_dataframes(self) -> Mapping[str, pd.DataFrame]:
        dataframes = {}
        for key, value in self.bindings.items():
            if isinstance(value, pd.DataFrame):
                dataframes[f"df_{id(value)}"] = value
            elif isinstance(value, SQL):
                dataframes.update(value.collect_dataframes())
        return dataframes


class DuckDB:
    def __init__(
        self,
        options="",
        db_location=":memory:",
        s3_storage_used=True,
        aws_profile="codenym",
    ):
        self.options = options
        self.db_location = db_location
        self.s3_storage_used = s3_storage_used
        self.aws_profile = aws_profile

    def connect(self):
        db = connect(self.db_location)
        if self.s3_storage_used:
            db.query("install httpfs; load httpfs;")
            db.query("install aws; load aws;")
            db.query(f"CALL load_aws_credentials('{self.aws_profile}');")
        db.query(self.options)
        return db

    def query(self, select_statement: SQL):
        db = self.connect()
        dataframes = select_statement.collect_dataframes()
        for key, value in dataframes.items():
            db.register(key, value)

        result = db.query(select_statement.to_string())
        if result is None:
            return
        return result.df()

    def __enter__(self):
        self.__connection = self.connect()
        return self.__connection

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.__connection.close()
        self.__connection = None

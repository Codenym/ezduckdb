from duckdb import connect
import pandas as pd
from sqlescapy import sqlescape
from string import Template
from typing import Mapping
from .paths import S3AwarePath


class SQL:
    """A class for handling SQL queries with dynamic bindings.

    This class allows for the creation of SQL queries with variable bindings. It supports various data types for these bindings,
    including dataframes, nested SQL queries, strings, and primitive types. The class provides functionality to convert the query
    with its bindings to a string and to collect dataframes associated with the query.

    Parameters
    ----------
    sql : str
        The SQL query string with placeholders for bindings
    **bindings : dict
        Variable keyword arguments representing the bindings for the SQL query. The keys are the placeholder names in the SQL query,
        and the values are the actual values to be bound to these placeholders.

    Methods
    -------
    to_string() -> str
        Converts the SQL query with its bindings to a string, with appropriate formatting and escaping of values.

    collect_dataframes() -> Mapping[str, pd.DataFrame]
        Collects and returns a mapping of dataframe identifiers to their respective pandas DataFrame objects from the bindings.

    Raises
    ------
    AssertionError
        If a binding name does not exist in the SQL query.
    ValueError
        If a binding is of an invalid type that cannot be converted to a string representation for the SQL query.

    Notes
    -----
    - The method `to_string` handles different data types by converting them to their appropriate string representations in the SQL query.
      For instance, dataframes are represented by a unique identifier, and strings are escaped properly.
    - The method `collect_dataframes` is useful for retrieving the dataframes involved in the SQL query, especially when dealing with nested SQL queries.

    Examples
    --------
    >>> query = SQL("SELECT * FROM users WHERE id = $id", id=123)
    >>> print(query.to_string())
    "SELECT * FROM users WHERE id = 123"

    >>> df = pd.DataFrame(...)
    >>> query = SQL("INSERT INTO data VALUES $data", data=df)
    >>> dfs = query.collect_dataframes()
    >>> print(dfs)
    {'df_<unique_id_of_df>': <corresponding_dataframe>}
    """

    def __init__(self, sql, **bindings):
        for binding in bindings:
            assert binding in sql
        self.sql = sql
        self.bindings = bindings

    def to_string(self) -> str:
        """Converts the SQL query with its bindings into a string format.

        This method processes the SQL query and its associated bindings to generate a final query string.
        It handles various types of bindings: DataFrames are referenced by unique identifiers, nested SQL objects
        are recursively converted to strings, strings and file paths are escaped, and primitive types are directly converted.
        Unsupported types raise a ValueError.

        Returns
        -------
        str
            The formatted SQL query string with all bindings appropriately replaced.

        Raises
        ------
        ValueError
            If a binding is of an unsupported type that cannot be converted into a string representation.

        Examples
        --------
        >>> query = SQL("SELECT * FROM data WHERE id = $id", id=123)
        >>> print(query.to_string())
        "SELECT * FROM data WHERE id = 123"

        >>> df = pd.DataFrame(...)
        >>> nested_query = SQL("SELECT * FROM ($subquery) AS sub", subquery=SQL("SELECT * FROM data"))
        >>> print(nested_query.to_string())
        "SELECT * FROM (SELECT * FROM data) AS sub"
        """
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
        """
        Collects and returns dataframes associated with the SQL bindings.

        This method iterates through the bindings of the SQL object to find and collect all pandas DataFrame objects.
        It also recursively collects dataframes from nested SQL objects. The dataframes are returned as a dictionary
        mapping unique identifiers (generated from the dataframe's memory addresses) to the dataframe objects.

        Returns
        -------
        Mapping[str, pd.DataFrame]
            A dictionary mapping unique identifiers to pandas DataFrame objects present in the SQL bindings.

        Examples
        --------
        >>> df1 = pd.DataFrame(...)
        >>> df2 = pd.DataFrame(...)
        >>> query = SQL("SELECT * FROM $df1 left join $df2 using(id)", df1=df1, df2=df2)
        >>> dfs = query.collect_dataframes()
        >>> for key in dfs:
        ...     print(f"{key}: {type(dfs[key])}")
        df_<unique_id_of_df1>: <class 'pandas.core.frame.DataFrame'>
        df_<unique_id_of_df2>: <class 'pandas.core.frame.DataFrame'>
        """
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

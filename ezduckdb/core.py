import os

from duckdb import connect
import pandas as pd
from sqlescapy import sqlescape
from string import Template
from typing import Mapping
from .paths import S3AwarePath
import logging


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

    @classmethod
    def from_file(cls, fpath: S3AwarePath, **bindings):
        """
        Creates an SQL object from a file, optionally incorporating additional bindings.

        This class method reads an SQL query from a file, allowing for initial bindings to be specified within the file and
        supplemented by additional bindings passed as keyword arguments. The method supports S3AwarePath as file paths, facilitating
        seamless integration with local and S3 file systems. It also handles an optional header in the file specifying initial bindings
        in a Python dictionary format. This enables dynamic construction of SQL objects with pre-defined or externally supplied bindings.

        Parameters
        ----------
        fpath : S3AwarePath
            The file path (supporting S3 paths) from which to read the SQL query.
        **bindings : dict, optional
            Additional variable keyword arguments representing bindings for the SQL query. These bindings will override any
            bindings specified within the file if they share the same keys.

        Returns
        -------
        SQL
            An instance of the SQL class initialized with the query and bindings from the file and additional bindings provided.

        Raises
        ------
        FileNotFoundError
            If the specified file path does not exist.
        SyntaxError
            If the initial bindings in the file are not in a valid Python dictionary format.

        Examples
        --------
        >>> query = SQL.from_file('path/to/query.sql', id=123)
        >>> print(query.to_string())
        The output depends on the contents of 'path/to/query.sql' and the additional binding provided.
        """
        with open(fpath, "r") as f:
            text = f.read()

        if bindings is None and text.startswith("--bindings:"):
            logging.warning(
                "No bindings provided though file indicates that some exist.  Bindings from file are:"
            )
            logging.warning(text.split("\n")[0])
        return SQL(text, **bindings)

    def to_file(self, fpath: S3AwarePath, templated=False):
        """
        Saves the SQL query to a file, with an option to include bindings as a header.

        This method writes the SQL query, and optionally its bindings, to a specified file. If the 'templated' parameter is set to True,
        the method saves the sql template with a head containing the bindings. If False, it converts to valid sql and saves the query.
        The file is saved in a location specified by the S3AwarePath, allowing compatibility with both local and S3 file systems.

        Parameters
        ----------
        fpath : S3AwarePath
            The file path (supporting S3 paths) where the SQL query will be written.
        templated : bool, optional
            If True saves the sql query template.  If False sales the sql query with bindings relaced, by default False.

        Examples
        --------
        >>> query = SQL("SELECT * FROM data WHERE id = $id", id=123)
        >>> query.to_file('path/to/output.sql', templated=True)
        The file 'path/to/output.sql' will contain the query with bindings as a header.
        ```sql
        --bindings: {"id":123}

        SELECT * FROM data WHERE id = $id
        ```
        >>> query = SQL("SELECT * FROM data WHERE id = $id", id=123)
        >>> query.to_file('path/to/output.sql', templated=False)
        The file 'path/to/output.sql' will contain the query with bindings as a header.
        ```sql
        SELECT * FROM data WHERE id = 123
        ```

        """
        if templated is True:
            file = """--bindings: {self.bindings}\n\n\n""" + self.sql
        elif templated is False:
            file = self.to_string()
        else:
            raise ValueError("templated must be a boolean")

        with open(fpath, "w") as f:
            f.write(file)

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
    """A class for managing connections and queries to a DuckDB database.

    This class provides an interface for connecting to a DuckDB database,
    executing queries, and managing the database connection. It supports
    integration with S3 storage using AWS credentials.

    Parameters
    ----------
    options : str, optional
        Additional options for the database connection, by default "".
    db_location : str, optional
        The location of the database. Use ':memory:' for in-memory database,
        by default ":memory:".
    aws_profile: str, optional
        Indicates the profile to use for aws authentication.
    aws_profile : str, optional
        The AWS profile name to be used for aws authentication.

    Notes
    ----------
    - Pick aws_profile or aws_env_vars. Not both.

    Attributes
    ----------
    options : str
        Options for the database connection.
    db_location : str
        The location of the DuckDB database.
    s3_storage_used : bool
        Flag to determine the usage of S3 storage.
    aws_profile : str
        The AWS profile name for accessing S3 storage.

    """

    def __init__(
        self, options="", db_location=":memory:", aws_profile=None, aws_env_vars=False
    ):
        self.options = options
        self.db_location = db_location
        self.aws_profile = aws_profile
        self.aws_env_vars = aws_env_vars

        if aws_profile and aws_env_vars:
            raise ValueError(
                "Cannot specify both aws_profile and aws_env_vars. Pick one."
            )

    def connect(self):
        """
        Establishes a connection to the DuckDB database.

        This method sets up the database connection based on the initialized
        parameters. If S3 storage is used, it installs and loads necessary
        extensions and sets the AWS credentials.

        Notes
        -----
        -  It is recommended to use the context manager or query method instead for most uses.

        Returns
        -------
        connection
            The connection object to the DuckDB database.

        Examples
        --------
        >>> duckdb_instance = DuckDB()
        >>> connection = duckdb_instance.connect()
        """
        connection = connect(self.db_location)
        if self.aws_profile or self.aws_env_vars:
            connection.query("install httpfs; load httpfs;")
            connection.query("install aws; load aws;")
            if self.aws_env_vars:
                connection.query("CALL load_aws_credentials();")
            else:
                connection.query(f"CALL load_aws_credentials('{self.aws_profile}');")
        connection.query(self.options)
        return connection

    def query(self, select_statement: SQL):
        """Executes a SQL query on the DuckDB database.

        This method connects to the database, registers dataframes from the
        provided SQL statement, and then executes the query.

        Parameters
        ----------
        select_statement : SQL
            An SQL object representing the SQL query to be executed.

        Returns
        -------
        DataFrame or None
            A pandas DataFrame containing the result of the query, or None
            if there is no result.

        Examples
        --------
        >>> duckdb_instance = DuckDB()
        >>> df = pd.DataFrame(...)
        >>> result = duckdb_instance.query(SQL("SELECT $value FROM $df",value=123,df=df))
        """
        db = self.connect()
        dataframes = select_statement.collect_dataframes()
        for key, value in dataframes.items():
            db.register(key, value)

        result = db.query(select_statement.to_string())
        if result is None:
            return
        return result.df()

    def __enter__(self):
        """Connects to the DuckDB database.

        The `with` statement will bind a duckdb connect return to
        the target specified in the as clause of the statement.

        Returns
        -------
        connection
            The connection object to the DuckDB database.

        Examples
        --------
        >>> with DuckDB() as connection:
        ...     # db is a connected database instance
        ...     result = connection.query("SELECT COUNT(*) FROM my_table;")
        """
        self.connection = self.connect()
        return self.connection

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Exit the runtime context and close the database connection.

        Parameters
        ----------
        exc_type : Exception or None
            The type of the exception that caused the context to be exited.
        exc_value : Exception or None
            The exception that caused the context to be exited.
        exc_tb : Traceback or Non e
            A traceback object.

        Examples
        --------
        >>> with DuckDB() as connection:
        ...     # Operations with db
        ... # Automatic closure of db connection occurs here
        """
        self.connection.close()

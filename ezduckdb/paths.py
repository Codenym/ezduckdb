from pathlib import Path


class S3AwarePath(type(Path())):
    """
    A subclass of pathlib.Path that adds additional methods for handling
    Amazon S3 paths.

    Methods
    -------
    is_s3()
        Checks if the path is an S3 path.

    __str__()
        Returns the string representation of the path, formatted correctly
        for S3 paths.

    get_s3_bucket()
        Extracts the bucket name from an S3 path.

    get_s3_prefix()
        Extracts the S3 prefix (the path inside the bucket) from an S3 path.

    get_table_name()
        Parses the schema and table name from the file stem assuming a
        specific naming convention of `<schema_name>_<table_name>.extension`.

    Raises
    ------
    Exception
        If the path is not an S3 path when calling `get_s3_bucket` or
        `get_s3_prefix`, or if the file stem does not follow the required
        naming convention when calling `get_table_name`.

    Examples
    --------
    >>> path = S3AwarePath("s3://mybucket/myfolder/myfile_name_here.parquet")
    >>> path.is_s3()
    True
    >>> str(path)
    's3://mybucket/myfolder/myfile.parquet'
    >>> path.get_s3_bucket()
    'mybucket'
    >>> path.get_s3_prefix()
    'myfolder/myfile.parquet'
    >>> path.get_table_name()
    ('myfile', 'name_here')
    """

    def is_s3(self):
        """
        Check if the path is an S3 path.

        Returns
        -------
        bool
            True if the path is an S3 path, False otherwise.
        """
        return self.parts[0] == "s3:"

    def __str__(self):
        if self.is_s3():
            return f"s3://{super().__str__()[4:]}"
        else:
            return super().__str__()

    def get_s3_bucket(self):
        """Extract the bucket name from an S3 path.

        Returns
        -------
        str
            The bucket name.

        Raises
        ------
        Exception
            If the path is not an S3 path.
        """
        if self.is_s3():
            return self.parts[1]
        else:
            raise Exception("Not an S3 path")

    def get_s3_prefix(self):
        """Extract the S3 prefix from an S3 path.

        Returns
        -------
        str
            The S3 prefix (the path inside the bucket).

        Raises
        ------
        Exception
            If the path is not an S3 path.
        """
        if self.is_s3():
            return "/".join(self.parts[2:])
        else:
            raise Exception("Not an S3 path")

    def get_table_name(self):
        """
        Parse the schema and table name from the file stem.

        Assumes a naming convention of `<schema_name>_<table_name>`.

        Returns
        -------
        tuple of str
            A tuple containing the schema name and table name.

        Raises
        ------
        Exception
            If the file stem does not follow the required naming convention.

        Examples
        --------
        >>> path = S3AwarePath("myfile_name_here.parquet")
        >>> path.get_table_name()
        ('myfile', 'name_here')
        """
        if self.stem.count("_") < 1:
            raise Exception(
                "Not a valid format. Needs at least 1 `_` to match format `<schema_name>_<table_name>`"
            )
        schema_name = self.stem.split("_")[0]
        table_name = self.stem[len(schema_name) + 1 :]
        return schema_name, table_name

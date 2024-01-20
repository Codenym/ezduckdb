"""
.. include:: ../README.md
"""
from .core import SQL, DuckDB
from .paths import S3AwarePath

__all__ = ["SQL", "DuckDB", "S3AwarePath"]

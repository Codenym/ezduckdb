# ezduckdb

Tools to make working with [duckdb](https://duckdb.org) easier for [codenym](https://codenym.github.io/website).

Not intended for general use, but feel free to steal code or ideas.

If you'd be super excited about this being made for general use, reach out.

## Acknowledgements

There's a ton code and ideas in here from a [dagster](https://dagster.io) [blog post](https://dagster.io/blog/duckdb-data-lake)

## Installation

```bash
pip install ezduckdb
```

## Usage

There are 3 classes in this library:

- `S3AwarePath`: `pathlib.Path` + s3 paths
- `SQL`: Work with sql files programatically via templating.
- `DuckDB`: Connection and Query manager

### S3AwarePath

`S3AwarePath` adds functionality to the `pathlib.Path` class.

- `is_s3`: Is path an s3 path (ie `s3://....`)
- `get_s3_bucket` and `get_s3_prefix`: Break path for use with boto3
- Retain `s3://` when cast to string (ie in f strings)
- `get_table_name`: Get db table name from file name based on codenym convention
  - `<schema>_<table>.<extension>`

```python
from ezduckdb import S3AwarePath

s3_path = S3AwarePath("s3://bucket/curated/s1chema_table1.csv")
assert inp.get_s3_bucket() == "bucket"
assert inp.get_s3_prefix() == "curated/s1chema_table1.csv"
assert str(inp) == "s3://bucket/curated/s1chema_table1.csv"
assert inp.is_s3()
assert inp.get_table_name() == ("s1chema", "table1")
```

### SQL

`SQL` enable type based templating for programatical sql query generation for duckdb.

Non-exhaustive list of replacements:

- `pd.DataFrame` is converted to `df_<id>` in the query to enable pandas querying
- `Str` are replaced with the string value enclosed in single quotes
- `Int` are replaced with the value without quotes
- `SQL` replaces recusively for nested querying

#### Basic

```python
from ezduckdb import SQL

example = SQL("SELECT * FROM $table WHERE id = $id", table="foo", id=1)
assert inp.to_string() == "SELECT * FROM 'foo' WHERE id = 1"
```

#### Pandas

```python
from ezduckdb import SQL
import pandas as pd

df = pd.DataFrame({"id": [1, 2, 3]})
inp = SQL("SELECT * FROM $table", table=df)
assert inp.to_string() == "SELECT * FROM df_" + str(id(df))
```

#### Nested

```python
from ezduckdb import SQL

example = SQL("SELECT * FROM $table", table=SQL("SELECT * FROM $table", table="foo"))
assert inp.to_string() == "SELECT * FROM (SELECT * FROM 'foo')"
```

### DuckDB

`DuckDB` is a connection manager for duckdb that has some convenience methods for querying.

- If `s3_storage_used=True` then `query` method will:
  - Load `httpfs` and `aws` duckdb extensions
  - call `load_aws_credentials` passing the `aws_profile`.
- `query` method will:
  - Do all sql templating for `SQL` object.
  - Return a `pd.DataFrame` of the results if applicable
- Provide a context manager for pure sql querying with strings

#### Templated Querying (Querying with `SQL` objects)

##### Basic Querying

```python
from ezduckdb import DuckDB
import pandas as pd

db = DuckDB(s3_storage_used=False)

assert db.query(SQL("select 1")).values == pd.DataFrame([(1,)]).values
```

##### Pandas Querying

```python
from ezduckdb import DuckDB
import pandas as pd

db = DuckDB(s3_storage_used=False)
df = pd.DataFrame({"id": [1, 2, 3]})

actual = db.query(SQL("SELECT * FROM $table", table=df))
expected = pd.DataFrame([(1,), (2,), (3,)])
assert (actual.values == expected.values).all()
```

##### S3 querying

```python
from ezduckdb import DuckDB
import pandas as pd

db = DuckDB(s3_storage_used=True)
s3_path = "s3://codenym-automated-testing/ezduckdb/parquet/schema1_table1.parquet"

actual = db.query(SQL("SELECT * FROM read_parquet($s3_path)", s3_path=s3_path))
expected = pd.DataFrame([[1, 4], [2, 5], [3, 6]])
assert (actual.values == expected.values).all()
```

#### Context Manager (Querying with Strings)

```python
from ezduckdb import DuckDB
import pandas as pd

with DuckDB(s3_storage_used=False) as conn:
    assert conn.query("select 1").df().values == pd.DataFrame([(1,)]).values
```

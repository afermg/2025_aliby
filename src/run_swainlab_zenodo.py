# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb",
#   "pooch",
# ]
# ///

from pathlib import Path

import duckdb
import pooch

url, md5 = [
    duckdb.sql(
        "SELECT url, md5 FROM read_csv(index.csv) WHERE is_meta = False"
    ).fetchnumpy()[key][0]
    for key in ("url", "md5")
]

data = pooch.retrieve(
    url, processor=pooch.Unzip() if ".zip" in url else None, known_hash=md5
)
zarr = Path(data[0]).parent

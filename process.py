import json
from typing import Final
from urllib.request import urlretrieve

import pandas as pd

from config import DATA_DIR, RESULT_DIR

# ----------------
# * Constants
# ----------------

SOURCE_DATA_URL: Final[str] = (
    "https://raw.githubusercontent.com/ihorh/Merchant-Category-Codes/refs/heads/main/With%20groups/mcc.json"
)

SOURCE_DATA_FILE: Final = DATA_DIR / "input" / "mcc.json"
MCC_FILE: Final = RESULT_DIR / "mcc.parquet"
MCC_GROUPS_FILE: Final = RESULT_DIR / "mcc_groups.parquet"

COL_TYPE: Final = "type"
COL_GROUP: Final = "group"
COL_SHORT_DESCR: Final = "shortDescription"
COL_FULL_DESCR: Final = "fullDescription"

# ----------------
# * Script
# ----------------

SOURCE_DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
RESULT_DIR.mkdir(parents=True, exist_ok=True)

if not SOURCE_DATA_FILE.exists():
    urlretrieve(SOURCE_DATA_URL, SOURCE_DATA_FILE)

with SOURCE_DATA_FILE.open(encoding="utf-8") as f:
    df = pd.DataFrame(json.load(f))

dfg = pd.json_normalize(df[COL_GROUP].to_list())
dfg = dfg.rename(columns={COL_TYPE: COL_GROUP})

df[COL_GROUP] = dfg[COL_GROUP]

dfg = dfg.drop_duplicates(keep="first", ignore_index=True)

df = df.join(pd.json_normalize(df.pop(COL_SHORT_DESCR).to_list()).rename(columns=lambda c: f"{COL_SHORT_DESCR}.{c}"))
df = df.join(pd.json_normalize(df.pop(COL_FULL_DESCR).to_list()).rename(columns=lambda c: f"{COL_FULL_DESCR}.{c}"))

dfg = dfg.rename(columns=lambda c: c.replace(".", "_"))  # parquet hates dots in column name
df = df.rename(columns=lambda c: c.replace(".", "_"))  # parquet hates dots in column name

dfg.to_parquet(MCC_GROUPS_FILE, engine="fastparquet", index=False)
df.to_parquet(MCC_FILE, engine="fastparquet", index=False)

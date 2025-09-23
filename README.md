# JupyterProjectTrain

Data generation and pipeline experiments.

## End-to-End Analytics Pipeline Proposal

### 1) Diagram: raw JSON to staging to analytics layer
```mermaid
flowchart LR
  subgraph Raw
    E[events.jsonl]
    U[users.csv]
    V[videos.csv]
    D[devices.csv]
  end

  E --> L[Landing / Raw storage]
  U --> L
  V --> L
  D --> L

  L --> S[Staging (Bronze/Silver)]
  S -->|normalize JSON, cast types, dedupe| C[Curated (Gold)]
  C --> A[Analytics Layer (Parquet/DuckDB/DB)]

  A --> N[Notebooks / BI]
```

### 2) How to make JSON queryable alongside CSV dimensions
- Read newline-delimited JSON (`events.jsonl`) with an explicit schema, flatten nested fields, and cast types to match dimension keys (`user_id`, `video_id`, `device_id`).
- Persist staged/curated data as columnar Parquet partitioned by date (e.g., `dt=YYYY-MM-DD`) for fast scans.
- Use a query engine (DuckDB locally or a warehouse) to join facts to CSV dimensions.

Minimal example (Pandas + DuckDB):
```python
import pandas as pd
import duckdb as ddb

# Load raw data
events = pd.read_json("data/events.jsonl", lines=True)
users = pd.read_csv("data/users.csv")
videos = pd.read_csv("data/videos.csv")
devices = pd.read_csv("data/devices.csv")

# If events contain nested objects/arrays, normalize here
# events = pd.json_normalize(events.to_dict(orient="records"))

# Ensure key and type alignment
# events["user_id"] = events["user_id"].astype(users["user_id"].dtype)

con = ddb.connect(database=":memory:")
con.register("events", events)
con.register("users", users)
con.register("videos", videos)
con.register("devices", devices)

joined = con.execute(
    """
    SELECT
      e.event_time,
      e.event_type,
      e.user_id,
      u.country,
      e.video_id,
      v.category,
      e.device_id,
      d.platform
    FROM events e
    LEFT JOIN users u   ON e.user_id   = u.user_id
    LEFT JOIN videos v  ON e.video_id  = v.video_id
    LEFT JOIN devices d ON e.device_id = d.device_id
    """
).df()
print(joined.head())
```

### 3) Final table/model structure (star schema)
- Fact table: `fact_video_events(event_id, event_time, user_id, video_id, device_id, event_type, session_id, dt)`
- Dimensions:
  - `dim_users(user_id, signup_date, country, ... )`
  - `dim_videos(video_id, title, category, creator_id, ... )`
  - `dim_devices(device_id, platform, app_version, ... )`
  - Optional: `dim_date(dt, y, q, m, d, dow, is_weekend, ... )`
- Derived marts (examples):
  - `daily_video_engagement(dt, video_id, plays, watch_time_sec, unique_viewers)`
  - `user_retention_cohorts(cohort_dt, dt, retained_users)`

Storage recommendations:
- Land raw files as-is (JSONL/CSV).
- Stage with type casting and deduplication.
- Curate to Parquet, partitioned by `dt` and optionally clustered by `video_id`.
- Expose via DuckDB or a warehouse for BI and notebooks.

## Goal
Enable dashboards and ad-hoc analysis so Product can answer:
Enable dashboards and ad-hoc analysis so Product can answer:
- Q1: “What % of new users reach at least 30 seconds of watch_time in their first session?”
- Q2: “Which video genres drive the highest 2nd-session retention within 3 days?”
- Q3: “Is there a particular device_os or app_version where drop-off is abnormally high?”


# Data generation and pipeline experiments.
TODO

### 1) Diagram: raw JSON to staging to analytics layer

```mermaid
flowchart TB
  %% Vertical, simple, larger blocks
  classDef big stroke-width:2px;

  E["Raw Events<br/>(events.jsonl)"]:::big
  U["Users<br/>(users.csv)"]:::big
  V["Videos<br/>(videos.csv)"]:::big
  D["Devices<br/>(devices.csv)"]:::big

  L["Landing<br/>(raw)"]:::big
  S["Staging<br/>(clean + typed)"]:::big
  C["Curated<br/>(facts + dims)"]:::big
  A["Analytics<br/>(DuckDB / Parquet)"]:::big
  N["Consumption<br/>(Notebooks / BI)"]:::big

  E --> L
  U --> L
  V --> L
  D --> L

  L --> S
  S --> C
  C --> A
  A --> N
```

### 2) How to make JSON queryable alongside CSV dimensions
- Use a query engine (DuckDB) to represent all data entities as SQL tables
- Get and output answer for Q1 through SQL-query
- Get and output answer for Q2 through SQL-query
- Get and output answer for Q3 through SQL-query

```mermaid
flowchart TD
  classDef big fill:#f3f4f6,stroke:#111,stroke-width:2px,color:#111,font-size:16px;

  S1["Use a query engine (DuckDB) to represent all data entities as SQL tables"]:::big
  S2["Get and output answer for Q1 through SQL-query"]:::big
  S3["Get and output answer for Q2 through SQL-query"]:::big
  S4["Get and output answer for Q3 through SQL-query"]:::big

  S1 --> S2
  S2 --> S3
  S3 --> S4
```

### 3) Final table/model structure 
- Final tables structure is same as input! We could extract required data by SQL-queries 


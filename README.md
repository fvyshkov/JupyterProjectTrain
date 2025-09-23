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
flowchart TD
  classDef big fill:#f3f4f6,stroke:#111,stroke-width:2px,color:#111,font-size:16px;

  S1["Use a query engine (DuckDB) to represent all data entities as SQL tables"]:::big
  S2["Get answer for Q1 through SQL-query"]:::big
  S3["Get answer for Q2 through SQL-query"]:::big
  S4["Get answer for Q3 through SQL-query"]:::big
  S5["Output all answers"]:::big

  S1 --> S2
  S2 --> S3
  S3 --> S4
  S4 --> S5
```

### 2) How to make JSON queryable alongside CSV dimensions
- Use a query engine (DuckDB) to represent all data entities as SQL tables

### 3) Final table/model structure 
- Final tables structure is same as input! We could extract required data by SQL-queries 


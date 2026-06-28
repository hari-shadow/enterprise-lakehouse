# Enterprise Lakehouse

A simplified replica of an enterprise data lakehouse pipeline — built to deepen understanding of modern data engineering tools and patterns encountered in professional work.

---

## Architecture

![Pipeline Architecture](docs/pipeline_architecture.png)

> Data flows from two cloud Postgres source databases through Airbyte CDC into AWS S3 as the raw Bronze layer. Databricks reads directly from S3 and dbt transforms the data through Silver and Gold medallion layers. Prefect Cloud orchestrates the full pipeline daily with no local infrastructure required.
---

## Tech Stack

| Tool | Role |
|---|---|
| [Neon Postgres](https://neon.tech) | CRM and ERP source databases |
| [Airbyte Cloud](https://airbyte.com) | CDC extraction → S3 |
| [AWS S3](https://aws.amazon.com/s3/) | Raw data lake |
| [Databricks](https://databricks.com) | Lakehouse compute + Unity Catalog |
| [dbt](https://getdbt.com) | SQL transformations |
| [Prefect Cloud](https://prefect.io) | Pipeline orchestration |
| [GitHub Actions](https://github.com/features/actions) | CI/CD |

---

## Data Model

### Sources
Two Neon Postgres databases simulate operational systems:

**CRM** — `customer_info`, `product_info`, `sales_info`

**ERP** — `erp_cust_az12` (demographics), `erp_loc_a101` (locations), `erp_px_cat_g1v2` (product categories)

### Gold Layer
```
dim_customers     → customer master with SCD Type 2 history
dim_products      → active product catalogue with category enrichment
fact_orders       → transactional orders joined to both dimensions
```

---
## Medallion Architecture

![Medallion Layers](docs/medallion_layers.png)


| Layer | Storage | Tool | Pattern |
|---|---|---|---|
| Bronze | AWS S3 (Parquet) | Airbyte | CDC — incremental sync |
| Silver | Databricks Unity Catalog | dbt | Incremental models with watermark |
| Gold | Databricks Unity Catalog | dbt | Full table rebuild |
| Snapshot | Databricks Unity Catalog | dbt | SCD Type 2 on dim_customers |


### Bronze — Raw Ingestion
Airbyte connects to two Neon Postgres databases (CRM and ERP) via CDC using logical replication slots. Raw data lands in AWS S3 as Parquet files, unchanged. Six tables are registered as external tables in Databricks Unity Catalog — data stays in S3, Databricks reads in place.

### Silver — Cleaning and Conforming
Six incremental dbt models transform the raw Bronze data. Each model applies:
- Deduplication using `row_number()` on natural keys
- Null and empty string filtering
- Date casting from integer format (`yyyyMMdd → date`)
- Code decoding (`M → Male`, `S → Single`, `M → Married`)
- Country standardisation and string trimming
- Sales amount validation and correction



### Gold — Business-Ready
Three dbt models produce the final analytical layer:

- **`dim_customers`** — customer master enriched with location (ERP) and demographics (ERP), surrogate key generated via `row_number()`
- **`dim_products`** — active product catalogue joined with category hierarchy, filtered to current records (`prd_end_date IS NULL`)
- **`fact_orders`** — transactional grain joined to both dimensions via surrogate keys

### Snapshot — SCD Type 2
`dim_customers` is tracked as a dbt snapshot using the `check` strategy on `country`, `gender`, and `marital_status`. Every attribute change closes the old row (`dbt_valid_to`) and inserts a new one (`dbt_valid_from`) — preserving full customer history for point-in-time analysis.

---


## Pipeline Flow

```
Neon Postgres (CRM)   ──┐
                        ├──► Airbyte CDC ──► AWS S3 (Bronze) ──► dbt Silver ──► dbt Gold + Snapshot
Neon Postgres (ERP)   ──┘

Orchestrated by Prefect Cloud — daily at 6AM IST, no local infrastructure required.
GitHub Actions runs dbt test on every pull request to main.
```

---

## Project Structure (simplified)

```
enterprise-lakehouse/
├── dataset/                            # Source CSV files (CRM + ERP)
├── scripts/
│   └── load_neon.py                    # Loads source data into Neon Postgres
├── databricks/
│   └── bronze_external_tables.sql      # Registers S3 Parquet as external tables
├── enterprise_lakehouse_dbt/
│   ├── models/
│   │   ├── silver/                     # 6 incremental models + sources.yml
│   │   └── gold/                       # dim_customers, dim_products, fact_orders
│   ├── snapshots/
│   │   └── dim_customers_snapshot.sql  # SCD Type 2
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── profiles.yml
├── prefect_flows/
│   ├── pipeline.py                     # Orchestration flow
│   └── prefect.yaml                    # Deployment config
└── .github/
    └── workflows/
        └── dbt_ci.yml                  # dbt test on PR
```

---

## Key Design Decisions

**CDC over full refresh**
Airbyte uses logical replication slots on Neon Postgres to capture only inserts, updates, and deletes — not re-reading the entire table on every sync.

**Incremental dbt models**
Silver models use a cross join CTE watermark pattern instead of subqueries in WHERE clauses — required for Databricks SQL compatibility.

**SCD Type 2**
`dim_customers` is tracked as a dbt snapshot with `check` strategy on `country`, `gender`, and `marital_status`. Every change creates a new versioned row with `dbt_valid_from` and `dbt_valid_to` timestamps — enabling point-in-time joins.

**External locations over internal storage**
Unlike Snowflake which copies data into its own storage, Databricks reads Parquet files directly from S3 via Unity Catalog external locations. Data stays in S3 — no vendor lock-in on storage.

**Prefect Managed Worker**
The orchestration agent runs entirely in Prefect's cloud infrastructure. No VM, no local terminal required.

---


## Setup

### Prerequisites
- Neon account (free tier)
- AWS account with S3 bucket
- Airbyte Cloud account (free trial)
- Databricks account (free edition)
- Prefect Cloud account (free tier)

### Environment Variables

```env
CRM_DB_URL=postgresql://user:pass@host/crm_db
ERP_DB_URL=postgresql://user:pass@host/erp_db
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-south-1
S3_BUCKET=enterprise-lakehouse-bronze
DATABRICKS_TOKEN=...
DATABRICKS_HOST=...
DATABRICKS_HTTP_PATH=...
AIRBYTE_CRM_CONNECTION_ID=...
AIRBYTE_ERP_CONNECTION_ID=...
```

### Installation
`requirements.txt` file is added.

```bash
git clone https://github.com/hari-shadow/enterprise-lakehouse.git
cd enterprise-lakehouse
python -m venv venv && venv\Scripts\activate
pip install psycopg2-binary pandas python-dotenv dbt-databricks prefect requests

python scripts/load_neon.py

cd enterprise_lakehouse_dbt
dbt run && dbt snapshot && dbt test
```
## CI/CD

Every pull request to `main` triggers a GitHub Actions workflow that runs `dbt test` against the live Databricks warehouse. A PR cannot be merged if any data quality test fails.

---

## Orchestration

The full pipeline runs automatically every day at **6AM IST** via Prefect Cloud:

1. Airbyte syncs CRM and ERP sources to S3 (parallel)
2. dbt runs Silver incremental models
3. dbt runs Gold full table rebuilds
4. dbt runs SCD Type 2 snapshot
5. dbt runs all data quality tests

No local machine or terminal is required — execution runs on Prefect's managed worker infrastructure.

---
> Built as a personal deep-dive to consolidate skills from professional work — a simplified replica of a real enterprise pipeline I worked on.
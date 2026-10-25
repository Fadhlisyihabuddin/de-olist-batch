# Olist E-Commerce Batch ELT Pipeline

An end-to-end, idempotent batch ELT data pipeline built with **Apache Airflow**, **AWS S3**, **PostgreSQL**, **dbt Core**, and **Metabase**, containerized with **Docker Compose**.

![Architecture Diagram](docs/architecture.svg)
> 💡 *The editable architecture diagram is available at [`docs/architecture.drawio`](docs/architecture.drawio) (can be opened directly in [draw.io / diagrams.net](https://app.diagrams.net/)).*

---

## 📌 Architecture & Data Flow

The pipeline ingests raw e-commerce data from Kaggle, loads it into a cloud data lake, stages it into a relational warehouse, models it into clean analytics marts, and serves it to BI dashboards:

```
[ Kaggle CSVs ] ──▶ [ AWS S3 Bronze ] ──▶ [ PostgreSQL Raw ] ──▶ [ dbt Core ] ──▶ [ Metabase ]
 (data/*.csv)       (s3://.../bronze/)      (schema: raw)       (staging/mart)     (Port 3000)
```

1. **Bronze Ingestion (`to_bronze.py`)**:
   - Uploads local CSVs (`orders`, `payments`, `customers`) to **AWS S3** under date partitions `bronze/{ds}/`.
   - Idempotent: Overwrites existing files for the partition key.
2. **Warehouse Staging (`to_postgres.py`)**:
   - Reads bronze data from S3 and loads into PostgreSQL `raw` schema.
   - Idempotent partition reload: Executes atomic `DELETE WHERE ds = '...'` before inserting, ensuring zero duplicates on DAG retries or backfills.
3. **Analytics Engineering (`dbt`)**:
   - **Staging (`stg_orders`, `stg_payments`)**: Type casting, initial filtering, materialized as views.
   - **Intermediate (`int_transactions`)**: Aggregates multi-payment splits per `order_id` to guarantee 1:1 order grain.
   - **Marts (`fct_transactions`)**: Materialized table combining order metadata with business logic:
     - Includes a banking/fintech **risk flag** (`is_risky`) based on high-installment splits (&ge; 10 installments) or oversized voucher payments.
   - **Data Quality**: Automated schema tests (`not_null`, `unique`, `accepted_values`).
4. **BI & Visualization (`Metabase`)**:
   - Serves analytical dashboards tracking daily GMV trends, order completion rates, and high-risk payment distributions.
5. **Orchestration (`Airflow`)**:
   - Daily schedule (`@daily`), task flow: `bronze_to_s3 >> load_raw_postgres >> dbt_run >> dbt_test`.

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 2.9 (LocalExecutor) | Pipeline scheduling, retries, and task dependencies |
| **Data Lake** | AWS S3 (Region: `ap-southeast-3`) | Partitioned cloud storage for raw/bronze data |
| **Data Warehouse** | PostgreSQL 16 | Relational storage for raw tables and dbt marts |
| **Transformation** | dbt Core 1.8+ (`dbt-postgres`) | Dimensional modeling, lineage, and schema testing |
| **Business Intelligence** | Metabase | Interactive dashboards and KPI reporting |
| **Infrastructure** | Docker & Docker Compose | Multi-container setup and custom Airflow image |

---

## 📁 Repository Structure

```
.
├── dags/
│   └── olist_daily.py          # Airflow DAG definition
├── dbt/
│   ├── dbt_project.yml         # dbt project configurations
│   ├── profiles.yml            # Database connection profile
│   └── models/
│       ├── staging/            # Staging views (stg_orders, stg_payments)
│       ├── intermediate/       # Grain alignment & aggregations (int_transactions)
│       └── marts/              # Fact tables (fct_transactions) & schema tests
├── docs/
│   ├── architecture.drawio     # Editable Draw.io architecture diagram
│   └── architecture.svg        # Architecture SVG preview
├── ingest/
│   ├── to_bronze.py            # Local CSV -> AWS S3 Bronze partition
│   └── to_postgres.py          # S3 Bronze -> PostgreSQL raw tables (idempotent)
├── .env.example                # Sample environment variables
├── Dockerfile                  # Custom Airflow image with pre-installed dependencies
├── docker-compose.yml          # Container configuration (Airflow, Postgres, Metabase)
├── requirements.txt            # Python dependencies (dbt-postgres, boto3, pandas)
└── README.md
```

---

## 🚀 Quickstart Guide

### 1. Prerequisites
- Docker Engine & Docker Compose installed.
- AWS Account with an S3 Bucket and IAM credentials (`AmazonS3FullAccess` or scoped bucket permissions).
- Olist dataset from [Kaggle Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

### 2. Environment Configuration
Clone the repository and copy the environment file:
```bash
cp .env.example .env
```
Edit `.env` with your AWS and database credentials:
```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=ap-southeast-3
S3_BUCKET=your-bucket-name

POSTGRES_USER=de
POSTGRES_PASSWORD=your_password
POSTGRES_DB=olist
```

### 3. Add Raw Data
Place the downloaded CSV files inside a `data/` folder at the project root:
- `data/olist_orders_dataset.csv`
- `data/olist_order_payments_dataset.csv`
- `data/olist_customers_dataset.csv`

### 4. Build & Launch Containers
```bash
docker compose up -d --build
```

### 5. Run Pipeline & Access Services
- **Airflow Webserver**: Open [http://localhost:8080](http://localhost:8080) (Default user: `admin` / `admin`).
  - Unpause DAG `olist_daily` and trigger a run.
- **Metabase BI**: Open [http://localhost:3000](http://localhost:3000).
  - Connect to PostgreSQL (`host: postgres`, `port: 5432`, database/user/password from `.env`).
  - Explore table `marts.fct_transactions`.

---

## ⚖️ Engineering Trade-offs & Production Design

* **PostgreSQL vs. Cloud DWH (BigQuery / Snowflake)**:
  * For datasets < 10 million rows, PostgreSQL is extremely cost-efficient (~$0 infrastructure cost locally or hosted on a small VPS) while supporting full SQL transformations.
  * **Upgrade Path**: The dbt layer is database-agnostic. Migrating to BigQuery or Snowflake only requires updating `profiles.yml` without rewriting business logic.
* **Real AWS S3 vs. MinIO**:
  * Utilizes real AWS S3 storage (`ap-southeast-3` Jakarta) to demonstrate production IAM role handling, bucket policies, and cloud partition layouts. Total storage cost is negligible (< $0.50/month).
* **Pre-baked Custom Dockerfile**:
  * Dependencies (`dbt-postgres`, `boto3`, `pandas`) are baked into the image at build time rather than installed at runtime via `command:`, preventing startup delay and container failure during network spikes.

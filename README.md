# Olist Batch ELT — portfolio Fadhli

`Kaggle CSV -> S3 (bronze) -> Postgres raw -> dbt (staging/intermediate/marts) -> Metabase`, orkestrasi Airflow. Idempotent daily.

```
            ┌──────┐   to_bronze.py   ┌────────────┐  to_postgres.py  ┌──────────┐  dbt run/test  ┌──────────┐
data/*.csv ─▶ lokal ────────────────▶ S3 bronze/ ──────────────────▶ Postgres raw ─────────────▶ marts ───▶ Metabase
            └──────┘                  └────────────┘  (DELETE+INSERT   └──────────┘               └──────────┘
                                                      per ds)        staging→inter→fct_transactions
Airflow DAG `olist_daily`: bronze_to_s3 >> load_raw_postgres >> dbt_run >> dbt_test
```

## Run (lokal dulu, VPS sama persis)
```bash
cp .env.example .env   # isi AWS key + S3_BUCKET + password
# download https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce -> taruh 3 CSV di data/
docker compose up -d
# Airflow: localhost:8080 (admin/admin) -> unpause olist_daily -> Trigger
# Metabase: localhost:3000 -> Add Postgres (host postgres, db/user/pass dari .env)
```

## Tradeoff
- Postgres bukan BigQuery: gratis, cukup untuk <10jt row, gampang di VPS. Upgrade path: ganti profile dbt ke BigQuery, model SQL sama.
- S3 real (bukan MinIO): biar nunjukin skill AWS beneran; biaya bronze CSV Olist < Rp5rb/bln.
- Risk flag di mart: contoh domain risk ala bank, gampang diceritain saat interview.

## Cost AWS/bulan
S3 <1GB + request Airflow: ~$0.50. VPS yang dominan.

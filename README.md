# Intraday-Market-Big-Data-Analysis

Big Data Analysis of Intraday Markets, from 01/26/2020 - 02/06/2026

Intended struct; Bronze Silver Gold

intraday-market-data-platform/
│
├── infra/
│ ├── config/
│ │ └── settings.yaml # paths, partitions, symbols
│ └── schemas/
│ ├── bronze_schema.json
│ ├── silver_schema.json
│ └── gold_schema.json
│
├── ingestion/
│ └── kaggle_ingest.py # Kaggle → Bronze
│
├── bronze/
│ └── write_raw_parquet.py # raw → partitioned parquet
│
├── silver/
│ ├── clean_prices.py # type casting, null handling
│ ├── normalize_times.py # timezone, trading hours
│ └── quality_checks.py # data validation
│
├── gold/
│ ├── fact_prices.py # fact table (long format)
│ ├── mart_intraday_metrics.py # rolling stats, returns
│ └── mart_correlations.py # cross-asset analytics
│
├── warehouse/
│ ├── ddl/
│ │ ├── fact_prices.sql
│ │ └── dim_time.sql
│ └── load_gold.py # load gold → warehouse
│
├── orchestration/
│ └── run_pipeline.py # end-to-end DAG runner
│
├── notebooks/
│ └── validation.ipynb # sanity checks only
│
├── README.md
├── requirements.txt
└── Makefile # one-command runs

# Plan

- Cache kaggle data
  - ingestion -> kaggle_ingest.py
  - path with version described in .env

# Error & Implementation notes

- Single file in Kaggle dataset not named correctly; Expliit renaming
  - .../TOS Kaggle data week ending 2024 09 013csv.csv -> .../TOS Kaggle data week ending 2024 09 13.csv

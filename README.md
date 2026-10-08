# FleetFuel Analytics

End-to-end data pipeline that turns Brazil's public fuel price survey (ANP) into a star schema on Databricks and a Power BI report that compares fuel prices across Brazil and shows where ethanol pays off over gasoline.

Built on **Databricks Free Edition** (serverless only), deployed as a **Databricks Asset Bundle**, and following the **medallion architecture** (raw → bronze → silver → gold) in **Unity Catalog**.

## Why

Diesel is one of the largest costs for any truck fleet. ANP publishes weekly prices from thousands of gas stations across Brazil, but only as raw monthly CSVs. This project answers three questions:

- What is the average diesel, gasoline and ethanol price today, by state and region?
- How have prices moved over the last 12 months?
- In which states is ethanol cheaper to run than gasoline?

## Architecture

```mermaid
flowchart LR
    ANP["ANP open data<br/>(monthly CSVs)"] -->|ingest| RAW["raw.files<br/>(UC Volume)"]
    RAW -->|bronze| BRONZE["bronze.prices_raw<br/>(all strings)"]
    BRONZE -->|silver| SILVER["silver.prices<br/>(typed, clean, deduplicated)"]
    SILVER -->|gold| GOLD["gold star schema"]
    GOLD -->|Import mode| PBI["Power BI report"]
```

A single job, `anp_fuel_pipeline`, runs every step on serverless compute. It is scheduled every Monday at 06:00 (America/Sao_Paulo), so new monthly files from ANP flow through to gold without manual work:

```mermaid
flowchart LR
    setup --> ingest --> bronze --> silver --> gold
    test --> silver
```

| Task | Source | What it does |
|---|---|---|
| `setup` | [`src/00_setup.sql`](src/00_setup.sql) | Creates the catalog, the `raw`/`bronze`/`silver`/`gold` schemas and the landing Volume. |
| `ingest` | [`src/01_ingest.py`](src/01_ingest.py) | Finds the ANP diesel and gasoline/ethanol CSVs on the open data page, from `start_year` up to the latest published month. Files already in the Volume are skipped. |
| `bronze` | [`src/02_bronze.py`](src/02_bronze.py) | Loads new CSVs into `bronze.prices_raw` as strings, with source file and load timestamp. Incremental and idempotent. |
| `test` | [`tests/run_tests.py`](tests/run_tests.py) | Runs the pytest suite for the silver rules on serverless. Silver does not run if a test fails. |
| `silver` | [`src/03_silver.py`](src/03_silver.py) | Renames columns to English, parses prices and dates, normalizes text, keeps DIESEL, DIESEL S10, GASOLINA and ETANOL, deduplicates. |
| `gold` | [`src/04_gold.sql`](src/04_gold.sql) | Rebuilds the star schema and a weekly aggregate. |

### Gold model

```mermaid
erDiagram
    fact_price }o--|| dim_date : date_key
    fact_price }o--|| dim_location : location_key
    fact_price }o--|| dim_product : product_key
    fact_price {
        int date_key
        int location_key
        int product_key
        decimal sale_price
        decimal purchase_price
        string station_id
    }
    dim_date {
        int date_key
        date date
        int week
        date week_start
        int month
        int quarter
        int year
    }
    dim_location {
        int location_key
        string city
        string state
        string region_code
        string region
    }
    dim_product {
        int product_key
        string product
        string unit
    }
```

`fact_price` has one row per station, product and survey day. `agg_price_weekly` holds the average, min and max price and the station count per state, region, product and week.

## Data quality

Each layer fails its task when a check does not pass, so bad data never reaches the report:

- **Bronze:** every loaded file has exactly as many rows as its CSV has data rows.
- **Silver:** sale price between R$ 1 and R$ 15 per liter, no null keys, and no more rows than bronze.
- **Gold:** no null keys in the fact table, every fact row matches its dimensions, and the fact table has the same row count as silver.
- **Unit tests:** the cleaning rules live in [`src/anp_fuel/silver_rules.py`](src/anp_fuel/silver_rules.py) as pure DataFrame functions. [`tests/test_silver_rules.py`](tests/test_silver_rules.py) tests them on small in-memory DataFrames.

## Power BI

The report connects to the Databricks SQL Warehouse in **Import** mode and reads only the `gold` schema. It is saved as a Power BI Project (`.pbip`), so the model and the report are plain text: the measures are in [`Medidas.tmdl`](powerbi/Fleet_Fuel_Analytics.SemanticModel/definition/tables/Medidas.tmdl).

- **Overview:** current average price, 12-month change, a map by state, and the difference to the national average.
- **Trend:** price over time by region and product.
- **Fuel Choice:** ethanol/gasoline price ratio by state and over time. Ethanol pays off when the ratio is below 70%, the usual break-even point given its lower energy content:

  `ratio = average ETANOL price ÷ average GASOLINA price`

## How to run

**Requirements:** a Databricks workspace (Free Edition works) and the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) authenticated to it.

1. Set `workspace.host` in [`databricks.yml`](databricks.yml) to your workspace URL.
2. Validate, deploy and run:

   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run anp_fuel_pipeline -t dev
   ```

3. Optional: override the bundle variables.

   ```bash
   databricks bundle deploy -t dev --var="catalog=my_catalog" --var="start_year=2024"
   ```

| Variable | Default | Description |
|---|---|---|
| `catalog` | `anp_fuel` | Unity Catalog catalog for all schemas. |
| `start_year` | `2023` | First ANP survey year to ingest. Ingestion always runs up to the current year. |

4. Open `powerbi/Fleet_Fuel_Analytics.pbip` in Power BI Desktop, point the Databricks data source to your workspace host and SQL Warehouse (*Transform data → Data source settings*), and refresh.

## Project structure

```
├── databricks.yml            # bundle definition and variables
├── resources/
│   └── anp_fuel_job.yml      # job with all tasks
├── src/
│   ├── 00_setup.sql
│   ├── 01_ingest.py
│   ├── 02_bronze.py
│   ├── 03_silver.py
│   ├── 04_gold.sql
│   └── anp_fuel/
│       └── silver_rules.py   # testable cleaning rules
├── tests/
│   ├── conftest.py
│   ├── run_tests.py          # runs pytest as a job task
│   └── test_silver_rules.py
└── powerbi/
    ├── Fleet_Fuel_Analytics.pbip
    ├── Fleet_Fuel_Analytics.Report/        # pages and visuals
    └── Fleet_Fuel_Analytics.SemanticModel/ # tables, relationships, measures (TMDL)
```

## Tech stack

Databricks (Unity Catalog, Delta Lake, serverless jobs, Asset Bundles) · PySpark · Spark SQL · pytest · Power BI (DAX)

## Data source

[ANP: Série histórica de preços de combustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis), published by Brazil's National Agency of Petroleum, Natural Gas and Biofuels under its open data policy.

-- Databricks notebook source
-- Builds the gold star schema (fact_price + dim_date, dim_location, dim_product) and agg_price_weekly
-- from <catalog>.silver.prices. Every table is fully rebuilt on each run.

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------

-- One row per calendar day in the silver date range; date_key is yyyyMMdd. week is the ISO week number,
-- week_start the Monday of that week (used by agg_price_weekly and the Power BI trend axis).
CREATE OR REPLACE TABLE gold.dim_date AS
WITH bounds AS (
  SELECT min(collected_on) AS first_day, max(collected_on) AS last_day FROM silver.prices
)
SELECT
  CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
  d AS date,
  weekofyear(d) AS week,
  date_trunc('WEEK', d)::DATE AS week_start,
  month(d) AS month,
  quarter(d) AS quarter,
  year(d) AS year
FROM bounds
LATERAL VIEW explode(sequence(first_day, last_day, INTERVAL 1 DAY)) AS d;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_location AS
SELECT
  CAST(row_number() OVER (ORDER BY state, city) AS INT) AS location_key,
  city,
  state,
  region_code,
  CASE region_code
    WHEN 'N' THEN 'North'
    WHEN 'NE' THEN 'Northeast'
    WHEN 'CO' THEN 'Center-West'
    WHEN 'SE' THEN 'Southeast'
    WHEN 'S' THEN 'South'
  END AS region
FROM (
  SELECT DISTINCT city, state, region AS region_code FROM silver.prices
);

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_product AS
SELECT
  CAST(row_number() OVER (ORDER BY product) AS INT) AS product_key,
  product,
  -- 'R$ / litro' -> 'R$/l'
  CASE WHEN unit = 'R$ / LITRO' THEN 'R$/l' ELSE unit END AS unit
FROM (
  SELECT product, max(unit) AS unit FROM silver.prices GROUP BY product
);

-- COMMAND ----------

-- Grain: one price survey per station, product and day.
CREATE OR REPLACE TABLE gold.fact_price AS
SELECT
  CAST(date_format(s.collected_on, 'yyyyMMdd') AS INT) AS date_key,
  l.location_key,
  p.product_key,
  s.sale_price,
  s.purchase_price,
  s.station_id
FROM silver.prices s
LEFT JOIN gold.dim_location l ON s.city = l.city AND s.state = l.state
LEFT JOIN gold.dim_product p ON s.product = p.product;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_price_weekly AS
SELECT
  l.state,
  l.region,
  pr.product,
  d.week_start,
  CAST(avg(f.sale_price) AS DECIMAL(10,3)) AS avg_price,
  min(f.sale_price) AS min_price,
  max(f.sale_price) AS max_price,
  count(DISTINCT f.station_id) AS station_count
FROM gold.fact_price f
JOIN gold.dim_date d ON f.date_key = d.date_key
JOIN gold.dim_location l ON f.location_key = l.location_key
JOIN gold.dim_product pr ON f.product_key = pr.product_key
GROUP BY l.state, l.region, pr.product, d.week_start;

-- COMMAND ----------

-- Quality checks (PRD): no null keys in the fact, every fact row finds its dimensions,
-- and the fact has exactly one row per silver row. assert_true fails the task on violation.
SELECT
  assert_true(count_if(f.date_key IS NULL OR f.location_key IS NULL OR f.product_key IS NULL) = 0,
              'fact_price has null keys'),
  assert_true(count_if(d.date_key IS NULL OR l.location_key IS NULL OR p.product_key IS NULL) = 0,
              'fact_price rows without a matching dimension row'),
  assert_true(count(*) = (SELECT count(*) FROM silver.prices),
              'fact_price row count differs from silver.prices')
FROM gold.fact_price f
LEFT JOIN gold.dim_date d ON f.date_key = d.date_key
LEFT JOIN gold.dim_location l ON f.location_key = l.location_key
LEFT JOIN gold.dim_product p ON f.product_key = p.product_key;

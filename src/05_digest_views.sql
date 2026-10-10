-- Databricks notebook source
-- Views behind the n8n weekly digest (automation/). "Latest week" is always the most recent week_start
-- in gold.agg_price_weekly, never today's date, so the digest follows whatever ANP has published.

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------

-- Views cannot take parameters, so the bundle variables land in this one-row table and the views read it.
CREATE OR REPLACE TABLE gold.digest_config AS
SELECT
  CAST(:ratio_threshold AS DECIMAL(4,2)) AS ratio_threshold,
  CAST(:spike_threshold AS DECIMAL(5,2)) AS spike_threshold_pct,
  CAST(:min_stations AS INT) AS min_stations;

-- COMMAND ----------

-- Per product: national average of the latest week, the week before and the same week 52 weeks earlier.
-- Averages every station price in fact_price (like the Power BI 'Avg Price' measure), not the state averages.
CREATE OR REPLACE VIEW gold.vw_weekly_national AS
WITH latest AS (
  SELECT max(week_start) AS week_start FROM gold.agg_price_weekly
),
weekly AS (
  SELECT
    p.product,
    d.week_start,
    CAST(avg(f.sale_price) AS DECIMAL(10,3)) AS avg_price,
    count(DISTINCT f.station_id) AS station_count
  FROM gold.fact_price f
  JOIN gold.dim_date d ON f.date_key = d.date_key
  JOIN gold.dim_product p ON f.product_key = p.product_key
  CROSS JOIN latest l
  WHERE d.week_start IN (l.week_start, l.week_start - INTERVAL 7 DAYS, l.week_start - INTERVAL 364 DAYS)
  GROUP BY p.product, d.week_start
)
SELECT
  cur.product,
  cur.week_start,
  cur.avg_price,
  prev.avg_price AS prev_avg_price,
  CAST(round((cur.avg_price - prev.avg_price) / prev.avg_price * 100, 2) AS DECIMAL(8,2)) AS wow_change_pct,
  yago.avg_price AS avg_price_52w_ago,
  CAST(round((cur.avg_price - yago.avg_price) / yago.avg_price * 100, 2) AS DECIMAL(8,2)) AS change_12m_pct,
  cur.station_count
FROM weekly cur
JOIN latest l ON cur.week_start = l.week_start
LEFT JOIN weekly prev ON prev.product = cur.product AND prev.week_start = l.week_start - INTERVAL 7 DAYS
LEFT JOIN weekly yago ON yago.product = cur.product AND yago.week_start = l.week_start - INTERVAL 364 DAYS;

-- COMMAND ----------

-- Per state and product: latest week vs the week before. in_ranking keeps low-sample states out of the
-- top increases; is_spike marks ranked states whose price rose by at least the spike threshold.
CREATE OR REPLACE VIEW gold.vw_weekly_state_changes AS
WITH latest AS (
  SELECT max(week_start) AS week_start FROM gold.agg_price_weekly
),
changes AS (
  SELECT
    cur.state,
    cur.region,
    cur.product,
    cur.week_start,
    cur.avg_price,
    prev.avg_price AS prev_avg_price,
    CAST(round((cur.avg_price - prev.avg_price) / prev.avg_price * 100, 2) AS DECIMAL(8,2)) AS wow_change_pct,
    cur.station_count,
    prev.station_count AS prev_station_count
  FROM gold.agg_price_weekly cur
  JOIN latest l ON cur.week_start = l.week_start
  LEFT JOIN gold.agg_price_weekly prev
    ON prev.state = cur.state AND prev.product = cur.product AND prev.week_start = l.week_start - INTERVAL 7 DAYS
)
SELECT
  c.*,
  coalesce(c.station_count >= cfg.min_stations AND c.prev_station_count >= cfg.min_stations, false) AS in_ranking,
  coalesce(
    c.station_count >= cfg.min_stations AND c.prev_station_count >= cfg.min_stations
      AND c.wow_change_pct >= cfg.spike_threshold_pct,
    false
  ) AS is_spike
FROM changes c
CROSS JOIN gold.digest_config cfg;

-- COMMAND ----------

-- Per state: ethanol/gasoline price ratio in the latest week and the week before. ethanol_pays is true
-- when the ratio is below the threshold (false when a state has no ethanol price that week); the
-- flipped_* flags mark states that crossed it this week.
CREATE OR REPLACE VIEW gold.vw_fuel_ratio_current AS
WITH latest AS (
  SELECT max(week_start) AS week_start FROM gold.agg_price_weekly
),
by_week AS (
  SELECT
    a.state,
    a.region,
    a.week_start,
    max(CASE WHEN a.product = 'ETANOL' THEN a.avg_price END) AS ethanol_price,
    max(CASE WHEN a.product = 'GASOLINA' THEN a.avg_price END) AS gasoline_price
  FROM gold.agg_price_weekly a
  CROSS JOIN latest l
  WHERE a.week_start IN (l.week_start, l.week_start - INTERVAL 7 DAYS)
    AND a.product IN ('ETANOL', 'GASOLINA')
  GROUP BY a.state, a.region, a.week_start
),
ratios AS (
  SELECT
    cur.state,
    cur.region,
    cur.week_start,
    cur.ethanol_price,
    cur.gasoline_price,
    CAST(round(cur.ethanol_price / cur.gasoline_price, 4) AS DECIMAL(6,4)) AS ratio,
    prev.ethanol_price AS prev_ethanol_price,
    prev.gasoline_price AS prev_gasoline_price,
    CAST(round(prev.ethanol_price / prev.gasoline_price, 4) AS DECIMAL(6,4)) AS prev_ratio
  FROM by_week cur
  JOIN latest l ON cur.week_start = l.week_start
  LEFT JOIN by_week prev ON prev.state = cur.state AND prev.week_start = l.week_start - INTERVAL 7 DAYS
)
SELECT
  r.*,
  coalesce(r.ratio < cfg.ratio_threshold, false) AS ethanol_pays,
  coalesce(r.prev_ratio < cfg.ratio_threshold, false) AS prev_ethanol_pays,
  coalesce(r.ratio < cfg.ratio_threshold AND r.prev_ratio >= cfg.ratio_threshold, false) AS flipped_to_ethanol,
  coalesce(r.ratio >= cfg.ratio_threshold AND r.prev_ratio < cfg.ratio_threshold, false) AS flipped_to_gasoline
FROM ratios r
CROSS JOIN gold.digest_config cfg;

-- COMMAND ----------

-- Sanity checks: the digest needs all four products and a previous week to compare against.
SELECT
  assert_true(count(*) = 4, 'vw_weekly_national should have one row per product'),
  assert_true(count_if(prev_avg_price IS NULL) = 0, 'vw_weekly_national has no previous week to compare')
FROM gold.vw_weekly_national;

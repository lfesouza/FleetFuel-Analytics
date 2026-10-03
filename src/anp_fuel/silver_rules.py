"""Cleaning rules that turn bronze.prices_raw into silver.prices.

Each function takes and returns a DataFrame (or Column), so the rules can be unit tested
on tiny DataFrames (tests/test_silver_rules.py) and composed in src/03_silver.py.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# Bronze (Portuguese snake_case) -> silver (English snake_case).
COLUMN_MAP = {
    "regiao_sigla": "region",
    "estado_sigla": "state",
    "municipio": "city",
    "revenda": "station_name",
    "cnpj_da_revenda": "station_id",
    "nome_da_rua": "street",
    "numero_rua": "street_number",
    "complemento": "complement",
    "bairro": "district",
    "cep": "zip_code",
    "produto": "product",
    "data_da_coleta": "collected_on",
    "valor_de_venda": "sale_price",
    "valor_de_compra": "purchase_price",
    "unidade_de_medida": "unit",
    "bandeira": "brand",
}

PRODUCTS_IN_SCOPE = ["DIESEL", "DIESEL S10", "GASOLINA"]

# Text columns that are upper-cased and stripped of extra spaces.
TEXT_COLUMNS = ["region", "state", "city", "station_name", "station_id", "district", "product", "unit", "brand"]

# One survey row per station, product and day.
DEDUP_KEYS = ["station_id", "product", "collected_on"]


def rename_columns(df: DataFrame) -> DataFrame:
    """Renames the bronze columns to English; columns not in COLUMN_MAP (e.g. _source_file) are kept as-is."""
    return df.select([F.col(c).alias(COLUMN_MAP.get(c, c)) for c in df.columns])


def parse_price(col_name: str) -> Column:
    """'6,49' -> 6.490 as decimal(10,3); blank or malformed values become null."""
    return F.expr(f"try_cast(replace(trim(`{col_name}`), ',', '.') AS DECIMAL(10,3))")


def parse_date(col_name: str) -> Column:
    """'02/01/2023' (dd/MM/yyyy) -> date; malformed values become null."""
    return F.expr(f"try_to_date(trim(`{col_name}`), 'dd/MM/yyyy')")


def normalize_text(col_name: str) -> Column:
    """'  sao   paulo ' -> 'SAO PAULO'; blank strings become null."""
    cleaned = F.upper(F.trim(F.regexp_replace(F.col(col_name), r"\s+", " ")))
    return F.when(cleaned == "", None).otherwise(cleaned)


def cast_and_normalize(df: DataFrame) -> DataFrame:
    """Applies the type and text rules to an already renamed DataFrame."""
    df = (
        df.withColumn("sale_price", parse_price("sale_price"))
        .withColumn("purchase_price", parse_price("purchase_price"))
        .withColumn("collected_on", parse_date("collected_on"))
    )
    for c in TEXT_COLUMNS:
        df = df.withColumn(c, normalize_text(c))
    return df


def filter_scope(df: DataFrame) -> DataFrame:
    """Keeps only the products in scope and rows that have a sale price."""
    return df.where(F.col("product").isin(PRODUCTS_IN_SCOPE) & F.col("sale_price").isNotNull())


def deduplicate(df: DataFrame) -> DataFrame:
    """Keeps one row per DEDUP_KEYS, preferring the latest load, then the highest price (deterministic)."""
    order = [F.col(c).desc_nulls_last() for c in ("_ingested_at", "_source_file", "sale_price") if c in df.columns]
    w = Window.partitionBy(*DEDUP_KEYS).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def clean(bronze: DataFrame) -> DataFrame:
    """Full bronze -> silver transformation."""
    return deduplicate(filter_scope(cast_and_normalize(rename_columns(bronze))))

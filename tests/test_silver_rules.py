import datetime
from decimal import Decimal

from anp_fuel.silver_rules import (
    COLUMN_MAP,
    cast_and_normalize,
    clean,
    deduplicate,
    filter_scope,
    normalize_text,
    parse_date,
    parse_price,
    rename_columns,
)

BRONZE_COLUMNS = list(COLUMN_MAP)


def bronze_row(**overrides):
    """A valid bronze row (all strings, as in bronze.prices_raw); override fields by bronze column name."""
    row = {
        "regiao_sigla": "NE",
        "estado_sigla": "AL",
        "municipio": "ARAPIRACA",
        "revenda": "POSTO TESTE LTDA",
        "cnpj_da_revenda": " 02.817.655/0001-82",
        "nome_da_rua": "RODOVIA AL 220",
        "numero_rua": "S/N",
        "complemento": None,
        "bairro": "PLANALTO",
        "cep": "57308-000",
        "produto": "DIESEL S10",
        "data_da_coleta": "02/01/2023",
        "valor_de_venda": "6,49",
        "valor_de_compra": None,
        "unidade_de_medida": "R$ / litro",
        "bandeira": "VIBRA ENERGIA",
        "_source_file": "/Volumes/anp_fuel/raw/files/2023/precos-diesel-gnv-01.csv",
    }
    row.update(overrides)
    return row


def bronze_df(spark, rows):
    columns = BRONZE_COLUMNS + ["_source_file"]
    schema = ", ".join(f"`{c}` string" for c in columns)
    return spark.createDataFrame([tuple(r[c] for c in columns) for r in rows], schema)


def single_column(spark, values, expr):
    df = spark.createDataFrame([(v,) for v in values], "v string")
    return [r[0] for r in df.select(expr("v")).collect()]


def test_rename_columns_maps_to_english_and_keeps_metadata(spark):
    df = rename_columns(bronze_df(spark, [bronze_row()]))
    assert df.columns == list(COLUMN_MAP.values()) + ["_source_file"]


def test_parse_price_handles_comma_blank_and_garbage(spark):
    assert single_column(spark, ["6,49", " 5,1 ", "", None, "abc"], parse_price) == [
        Decimal("6.490"),
        Decimal("5.100"),
        None,
        None,
        None,
    ]


def test_parse_date_reads_day_first(spark):
    assert single_column(spark, ["02/01/2023", "31/12/2025", "2023-01-02", None], parse_date) == [
        datetime.date(2023, 1, 2),
        datetime.date(2025, 12, 31),
        None,
        None,
    ]


def test_normalize_text_uppercases_and_collapses_spaces(spark):
    assert single_column(spark, ["  sao   paulo ", "Diesel S10", "   ", None], normalize_text) == [
        "SAO PAULO",
        "DIESEL S10",
        None,
        None,
    ]


def test_cast_and_normalize_trims_station_id(spark):
    row = cast_and_normalize(rename_columns(bronze_df(spark, [bronze_row()]))).first()
    assert row.station_id == "02.817.655/0001-82"
    assert row.sale_price == Decimal("6.490")
    assert row.collected_on == datetime.date(2023, 1, 2)


def test_filter_scope_keeps_products_in_scope_with_price(spark):
    rows = [
        bronze_row(produto="DIESEL"),
        bronze_row(produto="DIESEL S10"),
        bronze_row(produto="gasolina"),
        bronze_row(produto="GASOLINA ADITIVADA"),
        bronze_row(produto="ETANOL"),
        bronze_row(produto="GNV"),
        bronze_row(produto="DIESEL", valor_de_venda=""),
    ]
    df = filter_scope(cast_and_normalize(rename_columns(bronze_df(spark, rows))))
    assert sorted(r.product for r in df.collect()) == ["DIESEL", "DIESEL S10", "ETANOL", "GASOLINA"]


def test_deduplicate_keeps_one_row_per_station_product_day(spark):
    rows = [
        bronze_row(valor_de_venda="6,49"),
        bronze_row(valor_de_venda="6,59"),  # same station/product/day -> duplicate
        bronze_row(data_da_coleta="03/01/2023"),  # different day -> kept
        bronze_row(produto="DIESEL"),  # different product -> kept
    ]
    df = deduplicate(cast_and_normalize(rename_columns(bronze_df(spark, rows))))
    assert df.count() == 3
    kept = df.where("product = 'DIESEL S10' AND collected_on = '2023-01-02'").first()
    assert kept.sale_price == Decimal("6.590")


def test_clean_end_to_end(spark):
    rows = [
        bronze_row(),
        bronze_row(),  # exact duplicate
        bronze_row(produto="GNV"),  # out of scope
        bronze_row(valor_de_venda=None),
    ]
    df = clean(bronze_df(spark, rows))
    assert df.count() == 1

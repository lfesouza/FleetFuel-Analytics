# Databricks notebook source
# Downloads the ANP monthly fuel price CSVs (diesel and gasoline files) into the raw Volume, from start_year
# up to the current year. Files land at /Volumes/<catalog>/raw/files/<year>/precos-<group>-<month>.csv;
# files already present are skipped, so each run only fetches the months ANP published since the last run.

import datetime
import os
import re
import shutil
import tempfile
import urllib.request

dbutils.widgets.text("catalog", "anp_fuel")
dbutils.widgets.text("start_year", "2023")

catalog = dbutils.widgets.get("catalog")
start_year = int(dbutils.widgets.get("start_year"))
current_year = datetime.date.today().year

# File URLs are scraped from the listing page because ANP names them inconsistently from 2026 on
# (e.g. "02-cados-abertos-preco-gasolina-etanol.csv", "04-dados-abertos-precos-diesel-gnv" without extension).
LISTING_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"
LINK_PATTERN = re.compile(r'href="(https://[^"]*/shpc/dsan/(\d{4})/([^"]+))"')
# "diesel-gnv" carries DIESEL and DIESEL S10; "gasolina-etanol" carries GASOLINA and ETANOL. Other products are filtered in silver.
FILE_GROUPS = ["diesel-gnv", "gasolina-etanol"]
VOLUME_DIR = f"/Volumes/{catalog}/raw/files"

# COMMAND ----------


def fetch(url: str):
    return urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=120)


def download(url: str, dest: str) -> None:
    """Downloads url to local disk first, then copies to dest, so a failed download never leaves a partial CSV."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        with fetch(url) as response:
            shutil.copyfileobj(response, tmp, length=1024 * 1024)
    shutil.copyfile(tmp.name, dest)
    os.remove(tmp.name)


def parse_month(name: str) -> int:
    """'precos-diesel-gnv-03.csv' (up to 2025) or '03-dados-abertos-precos-...' (2026 on) -> 3."""
    match = re.match(r"(\d{2})-", name) or re.search(r"-(\d{2})\.csv$", name)
    if not match:
        raise ValueError(f"Cannot parse month from ANP file name: {name}")
    return int(match.group(1))


with fetch(LISTING_URL) as response:
    html = response.read().decode("utf-8")

# (year, group, month) -> url
files = {}
for url, year, name in LINK_PATTERN.findall(html):
    group = next((g for g in FILE_GROUPS if g in name), None)
    if group and start_year <= int(year) <= current_year:
        files[(int(year), group, parse_month(name))] = url

# Guard against a page layout change silently shrinking the data: every month from January of start_year
# up to the latest published month must have one file per group.
if not files:
    raise ValueError(f"No ANP files found on {LISTING_URL}")
latest = max((y, m) for (y, _, m) in files)
expected = {(y, g, m) for y in range(start_year, latest[0] + 1) for g in FILE_GROUPS for m in range(1, 13)}
missing = sorted(k for k in expected if (k[0], k[2]) <= latest and k not in files)
if missing:
    raise ValueError(f"Missing ANP files (year, group, month): {missing}")

# COMMAND ----------

downloaded, skipped = [], []
for (year, group, month), url in sorted(files.items()):
    os.makedirs(f"{VOLUME_DIR}/{year}", exist_ok=True)
    dest = f"{VOLUME_DIR}/{year}/precos-{group}-{month:02d}.csv"
    if os.path.exists(dest):
        skipped.append(dest)
        continue
    download(url, dest)
    downloaded.append(dest)
    print(f"downloaded {dest}")

print(f"{len(downloaded)} downloaded, {len(skipped)} already present; latest month on ANP: {latest[0]}-{latest[1]:02d}")

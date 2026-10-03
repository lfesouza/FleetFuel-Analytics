# Databricks notebook source
# Downloads the ANP monthly fuel price CSVs (diesel and gasoline files) into the raw Volume.
# Files land at /Volumes/<catalog>/raw/files/<year>/<file name>; files already present are skipped.

import os
import shutil
import tempfile
import urllib.request

dbutils.widgets.text("catalog", "anp_fuel")
dbutils.widgets.text("years", "2023,2024,2025")

catalog = dbutils.widgets.get("catalog")
years = [y.strip() for y in dbutils.widgets.get("years").split(",") if y.strip()]

BASE_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan"
# "diesel-gnv" carries DIESEL and DIESEL S10; "gasolina-etanol" carries GASOLINA. Other products are filtered in silver.
FILE_GROUPS = ["diesel-gnv", "gasolina-etanol"]
VOLUME_DIR = f"/Volumes/{catalog}/raw/files"

# COMMAND ----------


def download(url: str, dest: str) -> None:
    """Downloads url to local disk first, then copies to dest, so a failed download never leaves a partial CSV."""
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        with urllib.request.urlopen(request, timeout=120) as response:
            shutil.copyfileobj(response, tmp, length=1024 * 1024)
    shutil.copyfile(tmp.name, dest)
    os.remove(tmp.name)


downloaded, skipped = [], []
for year in years:
    os.makedirs(f"{VOLUME_DIR}/{year}", exist_ok=True)
    for group in FILE_GROUPS:
        for month in range(1, 13):
            name = f"precos-{group}-{month:02d}.csv"
            dest = f"{VOLUME_DIR}/{year}/{name}"
            if os.path.exists(dest):
                skipped.append(dest)
                continue
            download(f"{BASE_URL}/{year}/{name}", dest)
            downloaded.append(dest)
            print(f"downloaded {dest}")

print(f"{len(downloaded)} downloaded, {len(skipped)} already present")

# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # FIPE Brazilian Vehicle Prices Scraper
# MAGIC
# MAGIC Crawls the public FIPE JSON API (parallelum.com.br) brands -> models -> years -> price details, parses prices to numeric BRL, and merges the results into a Delta table keyed on fipe_code + model_year + reference_month. Defaults run a small test; set the limit widgets to 0 for a full crawl (many thousands of requests).

# COMMAND ----------

# DBTITLE 1,Widgets
# Parameters (0 = no limit). Defaults are a small test run.
dbutils.widgets.dropdown("vehicle_type", "carros", ["carros", "motos", "caminhoes"], "Vehicle type")
dbutils.widgets.text("brand_filter", "", "Brand filter (name contains or code)")
dbutils.widgets.text("model_filter", "", "Model filter (name contains, comma-separated)")
dbutils.widgets.text("max_brands", "2", "Max brands (0 = all)")
dbutils.widgets.text("max_models", "3", "Max models per brand (0 = all)")
dbutils.widgets.text("max_years", "0", "Max years per model (0 = all)")
dbutils.widgets.text("sleep_seconds", "0.5", "Sleep between calls (s)")
dbutils.widgets.text("catalog", "main", "Target catalog")
dbutils.widgets.text("schema", "default", "Target schema")
dbutils.widgets.text("table", "fipe_prices", "Target table")
dbutils.widgets.dropdown("write_mode", "merge", ["merge", "append"], "Write mode")

VEHICLE_TYPE = dbutils.widgets.get("vehicle_type")
BRAND_FILTER = dbutils.widgets.get("brand_filter").strip()
MODEL_FILTER = [t.strip().lower() for t in dbutils.widgets.get("model_filter").split(",") if t.strip()]
MAX_BRANDS = int(dbutils.widgets.get("max_brands") or 0)
MAX_MODELS = int(dbutils.widgets.get("max_models") or 0)
MAX_YEARS = int(dbutils.widgets.get("max_years") or 0)
SLEEP_SECONDS = float(dbutils.widgets.get("sleep_seconds") or 0.5)
TARGET_TABLE = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}.{dbutils.widgets.get('table')}"
WRITE_MODE = dbutils.widgets.get("write_mode")

print(f"type={VEHICLE_TYPE} brand_filter='{BRAND_FILTER}' model_filter={MODEL_FILTER} max_brands={MAX_BRANDS} "
      f"max_models={MAX_MODELS} max_years={MAX_YEARS} sleep={SLEEP_SECONDS}s target={TARGET_TABLE} mode={WRITE_MODE}")

# COMMAND ----------

# DBTITLE 1,HTTP client and parsing helpers
import re
import time
import random
from decimal import Decimal, InvalidOperation
import requests

BASE_URL = "https://parallelum.com.br/fipe/api/v1"
RETRY_STATUS = {429, 500, 502, 503, 504}

session = requests.Session()
session.headers.update({"User-Agent": "fipe-databricks-scraper/1.0", "Accept": "application/json"})

stats = {"requests": 0, "retries": 0, "failures": 0}

def get_json(path, max_retries=6, timeout=(5, 30), base_backoff=2.0):
    """GET BASE_URL+path with polite sleep, timeouts, and exponential backoff on 429/5xx/network errors."""
    url = f"{BASE_URL}{path}"
    for attempt in range(max_retries + 1):
        time.sleep(SLEEP_SECONDS)
        stats["requests"] += 1
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code not in RETRY_STATUS:
                resp.raise_for_status()
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if retry_after and retry_after.isdigit() else base_backoff * 2 ** attempt
            reason = f"HTTP {resp.status_code}"
        except (requests.ConnectionError, requests.Timeout) as e:
            wait, reason = base_backoff * 2 ** attempt, type(e).__name__
        if attempt == max_retries:
            break
        wait = min(wait, 120) + random.uniform(0, 1)
        stats["retries"] += 1
        print(f"  retry {attempt + 1}/{max_retries} for {path} after {reason}; sleeping {wait:.1f}s")
        time.sleep(wait)
    stats["failures"] += 1
    raise RuntimeError(f"Failed after {max_retries} retries: {url}")

def parse_brl(text):
    """'R$ 45.678,00' -> Decimal('45678.00'); returns None if unparseable."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d,.-]", "", text).replace(".", "").replace(",", ".")
    try:
        return Decimal(cleaned).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None

# Quick self-check of the price parser
assert parse_brl("R$ 45.678,00") == Decimal("45678.00")
assert parse_brl("R$ 1.234.567,89") == Decimal("1234567.89")
assert parse_brl("R$ 999,50") == Decimal("999.50")
print("parse_brl OK")

# COMMAND ----------

# DBTITLE 1,Crawler
from datetime import datetime, timezone

def crawl(vehicle_type, brand_filter="", max_brands=0, max_models=0, max_years=0, model_filter=None):
    rows, errors = [], []
    brands = get_json(f"/{vehicle_type}/marcas")
    if brand_filter:
        f = brand_filter.lower()
        brands = [b for b in brands if f in b["nome"].lower() or f == str(b["codigo"])]
    if max_brands:
        brands = brands[:max_brands]
    print(f"Crawling {len(brands)} brand(s)")

    for b in brands:
        b_code, b_name = str(b["codigo"]), b["nome"]
        try:
            models = get_json(f"/{vehicle_type}/marcas/{b_code}/modelos")["modelos"]
        except Exception as e:
            errors.append((b_code, None, None, str(e))); continue
        if model_filter:
            # keep models whose name contains any of the terms (case-insensitive), e.g. ["fit"]
            models = [m for m in models if any(t in m["nome"].lower() for t in model_filter)]
        if max_models:
            models = models[:max_models]
        print(f"- {b_name} ({b_code}): {len(models)} model(s)")

        for m in models:
            m_code, m_name = str(m["codigo"]), m["nome"]
            try:
                years = get_json(f"/{vehicle_type}/marcas/{b_code}/modelos/{m_code}/anos")
            except Exception as e:
                errors.append((b_code, m_code, None, str(e))); continue
            if max_years:
                years = years[:max_years]

            for y in years:
                y_code = str(y["codigo"])
                try:
                    d = get_json(f"/{vehicle_type}/marcas/{b_code}/modelos/{m_code}/anos/{y_code}")
                except Exception as e:
                    errors.append((b_code, m_code, y_code, str(e))); continue
                price_text = d.get("Valor")
                rows.append({
                    "vehicle_type": vehicle_type,
                    "brand_code": b_code,
                    "brand": d.get("Marca", b_name),
                    "model_code": m_code,
                    "model": d.get("Modelo", m_name),
                    "year_code": y_code,
                    "model_year": int(d["AnoModelo"]) if d.get("AnoModelo") is not None else None,  # 32000 = zero km
                    "fuel": d.get("Combustivel"),
                    "fipe_code": d.get("CodigoFipe"),
                    "reference_month": (d.get("MesReferencia") or "").strip(),
                    "price_text": price_text,
                    "price_brl": parse_brl(price_text),
                    "scraped_at": datetime.now(timezone.utc),
                })
    return rows, errors

# COMMAND ----------

# DBTITLE 1,Run crawl and build Spark DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               DecimalType, TimestampType)

t0 = time.time()
rows, errors = crawl(VEHICLE_TYPE, BRAND_FILTER, MAX_BRANDS, MAX_MODELS, MAX_YEARS, MODEL_FILTER)
print(f"\nCollected {len(rows)} price rows, {len(errors)} error(s) in {time.time() - t0:.0f}s; stats={stats}")
for e in errors[:10]:
    print("  error:", e)

schema = StructType([
    StructField("vehicle_type", StringType()),
    StructField("brand_code", StringType()),
    StructField("brand", StringType()),
    StructField("model_code", StringType()),
    StructField("model", StringType()),
    StructField("year_code", StringType()),
    StructField("model_year", IntegerType()),
    StructField("fuel", StringType()),
    StructField("fipe_code", StringType()),
    StructField("reference_month", StringType()),
    StructField("price_text", StringType()),
    StructField("price_brl", DecimalType(14, 2)),
    StructField("scraped_at", TimestampType()),
])
cols = [f.name for f in schema.fields]
fipe_df = spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], schema)

# Dedupe within the batch on the merge key (FIPE can list the same code/year under multiple models)
fipe_df = fipe_df.dropDuplicates(["fipe_code", "model_year", "reference_month"])
display(fipe_df.orderBy("brand", "model", "model_year"))

# COMMAND ----------

# DBTITLE 1,Write to Delta (merge or append)
import os
import pandas as pd

KEY = ["fipe_code", "model_year", "reference_month"]
# Fallback location used only when Unity Catalog is unavailable (no metastore assigned to the workspace)
FALLBACK_DIR = f"/Workspace/Users/{spark.sql('SELECT current_user()').first()[0]}/fipe_output"
FALLBACK_PATH = f"{FALLBACK_DIR}/{dbutils.widgets.get('table')}.parquet"

def write_delta():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
      vehicle_type STRING, brand_code STRING, brand STRING, model_code STRING, model STRING,
      year_code STRING, model_year INT, fuel STRING, fipe_code STRING, reference_month STRING,
      price_text STRING, price_brl DECIMAL(14,2), scraped_at TIMESTAMP
    ) USING DELTA
    COMMENT 'FIPE vehicle price index scraped from parallelum.com.br FIPE API'
    """)
    if WRITE_MODE == "append":
        fipe_df.write.mode("append").saveAsTable(TARGET_TABLE)
    else:
        fipe_df.createOrReplaceTempView("fipe_batch")
        display(spark.sql(f"""
        MERGE INTO {TARGET_TABLE} t
        USING fipe_batch s
        ON t.fipe_code = s.fipe_code AND t.model_year = s.model_year AND t.reference_month = s.reference_month
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """))

def write_fallback_parquet():
    os.makedirs(FALLBACK_DIR, exist_ok=True)
    # Build from the crawled Python rows (avoids Spark Connect toPandas serialization issues)
    new = pd.DataFrame(rows, columns=cols).drop_duplicates(KEY, keep="last")
    new["price_brl"] = new["price_brl"].astype("float64")
    if os.path.exists(FALLBACK_PATH):
        old = pd.read_parquet(FALLBACK_PATH)
        combined = pd.concat([old, new], ignore_index=True)
        # merge semantics: latest scrape wins per key; append keeps everything
        if WRITE_MODE == "merge":
            combined = combined.sort_values("scraped_at").drop_duplicates(KEY, keep="last")
    else:
        combined = new
    combined.to_parquet(FALLBACK_PATH, index=False)
    return len(new), len(combined)

try:
    write_delta()
    OUTPUT_SINK = "delta"
    print(f"Wrote {fipe_df.count()} rows to Delta table {TARGET_TABLE} ({WRITE_MODE})")
except Exception as e:
    if "METASTORE_DOES_NOT_EXIST" not in str(e):
        raise
    print("Unity Catalog metastore not assigned to this workspace -> falling back to Parquet in workspace files.")
    n_batch, n_total = write_fallback_parquet()
    OUTPUT_SINK = "parquet"
    print(f"Wrote {n_batch} batch rows to {FALLBACK_PATH} ({WRITE_MODE}); file now has {n_total} rows")

# COMMAND ----------

# DBTITLE 1,Validate table: counts and preview
if OUTPUT_SINK == "delta":
    tbl = spark.table(TARGET_TABLE)
else:
    _pdf = pd.read_parquet(FALLBACK_PATH)
    _recs = [(*r[:6], int(r[6]) if pd.notna(r[6]) else None, *r[7:11], Decimal(str(round(r[11], 2))) if pd.notna(r[11]) else None, r[12].to_pydatetime())
             for r in _pdf[cols].itertuples(index=False)]
    tbl = spark.createDataFrame(_recs, schema)
tbl.createOrReplaceTempView("fipe_out")

display(spark.sql("""
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT fipe_code, model_year, reference_month) AS distinct_keys,
       COUNT(DISTINCT brand) AS brands,
       COUNT(DISTINCT model) AS models,
       SUM(CASE WHEN price_brl IS NULL THEN 1 ELSE 0 END) AS null_prices,
       MIN(price_brl) AS min_price, MAX(price_brl) AS max_price,
       MAX(scraped_at) AS last_scraped_at
FROM fipe_out
"""))
display(spark.sql("""
SELECT vehicle_type, brand, reference_month, COUNT(*) AS rows, ROUND(AVG(price_brl), 2) AS avg_price_brl
FROM fipe_out GROUP BY ALL ORDER BY brand
"""))
display(tbl.orderBy("brand", "model", "model_year").limit(20))
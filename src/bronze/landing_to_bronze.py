"""
Step 7 - Bronze stage.

Reads the company list CSV files from the landing volume and saves them as the
bronze Delta table. Values are kept exactly as they appear in the files (all
strings); cleaning and normalization happen in the gold stage.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LANDING_PATH = "/Volumes/equipmentcompanies/landing/equipmentcompanies/"
BRONZE_TABLE = "equipmentcompanies.bronze.equipment_companies"

# CSV header -> bronze column name. Delta rejects spaces in column names.
COLUMN_NAMES = {
    "Serial No": "serial_no",
    "Company Name": "company_name",
    "Company Site URL": "company_site_url",
}


def clean_header(name):
    # Excel can save CSVs with a byte order mark in front of the first header.
    return name.lstrip("﻿").strip()


def main():
    spark = SparkSession.builder.getOrCreate()

    raw = (
        spark.read
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .csv(LANDING_PATH)
        .select("*", F.col("_metadata.file_path").alias("_source_file"))
    )
    raw = raw.toDF(*[clean_header(c) for c in raw.columns])

    missing = sorted(set(COLUMN_NAMES) - set(raw.columns))
    if missing:
        raise ValueError(f"Landing CSV is missing columns {missing}; found {raw.columns}")

    bronze = raw.select(
        *[F.col(f"`{src}`").alias(dst) for src, dst in COLUMN_NAMES.items()],
        "_source_file",
        F.current_timestamp().alias("_ingested_at"),
    )

    row_count = bronze.count()
    if row_count == 0:
        raise ValueError(f"No rows found in {LANDING_PATH}; bronze table was not changed")

    (
        bronze.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )
    print(f"Wrote {row_count} rows to {BRONZE_TABLE}")


if __name__ == "__main__":
    main()

"""
Step 9 - Gold stage.

Builds the dashboard tables in equipmentcompanies.gold from the silver tables:
  dim_company         one row per company: website, tagline, HQ address, phone,
                      map coordinates, founded year, data status
  company_categories  company x standard equipment category, with product pages
  electric_products   one row per electric or battery product
  tech_mentions       company x technology theme, with % of pages mentioning it
  news_by_year        company x year (last 5 years), with news pages

Cleaning: pages that redirected to another website and pages repeating another
page's text are dropped; non-product pages listed under product sections are
dropped; company names, cities, categories and product names are normalized.
Standard categories are defined in categories.py. Tables are replaced on every run.
"""
import inspect
import os
import sys
from datetime import date
from functools import reduce

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# Databricks runs script tasks with exec(), so __file__ is not set; the code
# object still knows the script's path. Needed to import categories.py.
sys.path.insert(0, os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))
from categories import CATEGORY_RULES, ELECTRIC, ELECTRIC_LANDING, NOT_PRODUCT  # noqa: E402

BRONZE = "equipmentcompanies.bronze"
SILVER = "equipmentcompanies.silver"
GOLD = "equipmentcompanies.gold"

NEWS_YEARS = list(range(date.today().year - 4, date.today().year + 1))
MIN_WORDS = 20        # pages with fewer words count as having no text
MIN_TEXT_PAGES = 20   # companies with fewer text pages get no technology data

TECH_THEMES = {
    "Electric & Battery": r"(?i)\b(electric|battery|batteries|electrified|zero[- ]emissions?)\b",
    "Autonomy": r"(?i)\b(autonomous|autonomy|driverless|self-driving)\b",
    "Telematics & Connectivity": r"(?i)\b(telematics|connectivity|remote monitoring|fleet management)\b",
    "GPS & Precision": r"(?i)\b(gps|precision ag\w*|grade control|machine control)\b",
    "Alternative Fuels": r"(?i)\b(hydrogen|hvo|renewable diesel|biodiesel)\b",
    "Emissions": r"(?i)\b(tier 4|tier iv|stage v|emissions?)\b",
    "Sustainability": r"(?i)\b(sustainab\w+|carbon|net[- ]zero)\b",
}

DATA_NOTES = {
    "ok": "Full data",
    "partial": "Partial: some pages failed",
    "js_rendered": "Limited: pages built by JavaScript",
    "captcha": "No data: website shows a captcha",
    "blocked": "No data: website blocks automated access",
    "error": "No data: website did not respond",
}


# Legal or regional suffixes dropped to get a short brand name for chart labels:
# "Kubota Tractor Corporation" -> "Kubota", "JCB North America" -> "JCB".
BRAND_SUFFIX = (r"\s+(Construction Equipment|Construction Machinery|Tractor Corporation|North America|"
                r"America|Corporation|Company|Agriculture|Inc\.?|LLC)$")


def clean(col):
    """Trim and collapse whitespace."""
    return F.trim(F.regexp_replace(col, r"\s+", " "))


def save(df, table):
    name = f"{GOLD}.{table}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    print(f"Wrote {df.sparkSession.table(name).count()} rows to {name}")


def standard_category(path):
    """Struct(category, segment) of the first matching rule in categories.py, or null."""
    first, *rest = CATEGORY_RULES

    def as_struct(rule):
        return F.struct(F.lit(rule[0]).alias("category"), F.lit(rule[1]).alias("segment"))

    return reduce(lambda acc, rule: acc.when(path.rlike(rule[2]), as_struct(rule)),
                  rest, F.when(path.rlike(first[2]), as_struct(first)))


def companies(spark):
    name = clean(F.col("company_name"))
    return spark.table(f"{BRONZE}.equipment_companies").select(
        F.col("serial_no").cast("int").alias("serial_no"),
        name.alias("company_name"),
        F.regexp_replace(name, BRAND_SUFFIX, "").alias("brand"),
        F.lower(clean(F.col("company_site_url"))).alias("website"),
    )


def clean_pages(spark):
    """OK pages that stayed on the company's site, without repeated texts."""
    base = spark.table(f"{SILVER}.scrape_status").select(
        "serial_no", F.regexp_replace(F.lower("base_url"), "/$", "").alias("base"))
    pages = (
        spark.table(f"{SILVER}.site_pages").join(base, "serial_no")
        .where(F.col("fetch_status") == "ok")
        .where(F.lower("final_url").startswith(F.col("base")))
        .withColumn("text_hash", F.sha2(F.coalesce("body_text", F.col("url")), 256))
    )
    first = Window.partitionBy("serial_no", "text_hash").orderBy("url")
    return (
        pages.withColumn("n", F.row_number().over(first))
        .where(F.col("n") == 1)
        .withColumn("serial_no", F.col("serial_no").cast("int"))
        .drop("n", "text_hash", "base")
    )


def product_urls(spark):
    """Product page addresses with their standard category (null when unmapped)."""
    return (
        spark.table(f"{SILVER}.site_urls")
        .where(F.col("page_type") == "product")
        .where(~F.col("relative_path").rlike(NOT_PRODUCT))
        .withColumn("serial_no", F.col("serial_no").cast("int"))
        .withColumn("std", standard_category(F.col("relative_path")))
        .select("serial_no", "url", "relative_path", "std.category", "std.segment")
    )


def build_dim_company(spark, company, pages, products):
    contacts = spark.table(f"{SILVER}.company_contacts").withColumn("serial_no", F.col("serial_no").cast("int"))
    status = spark.table(f"{SILVER}.scrape_status").select(
        F.col("serial_no").cast("int").alias("serial_no"), F.col("status").alias("data_status"))
    home = pages.where(F.col("page_type") == "home").select(
        "serial_no",
        clean(F.coalesce("meta_description", "h1", F.substring("body_text", 1, 200))).alias("tagline"))
    notes = F.create_map(*[F.lit(x) for kv in DATA_NOTES.items() for x in kv])
    product_counts = products.groupBy("serial_no").agg(F.countDistinct("url").alias("product_pages"))
    return (
        company.join(contacts.drop("company_name"), "serial_no", "left")
        .join(status, "serial_no", "left")
        .join(home, "serial_no", "left")
        .join(product_counts, "serial_no", "left")
        .select(
            "serial_no", "company_name", "brand", "website", "tagline",
            "hq_address", F.initcap(clean(F.col("city"))).alias("city"), F.upper("state").alias("state"), "zip",
            "latitude", "longitude", "phone", "founded_year",
            "address_match", "phone_match", "address_source_url", "phone_source_url",
            "data_status", F.coalesce(notes[F.col("data_status")], F.col("data_status")).alias("data_note"),
            F.coalesce("product_pages", F.lit(0)).alias("product_pages"),
            F.col("latitude").isNotNull().alias("has_location"),
        )
    )


def build_company_categories(company, products):
    unmapped = products.where(F.col("category").isNull())
    print(f"Product pages without a standard category: {unmapped.count()} of {products.count()}")
    (unmapped.groupBy("serial_no", F.regexp_extract("relative_path", r"^([^/]+(/[^/]+)?)", 1).alias("path"))
     .count().orderBy(F.desc("count")).show(15, truncate=False))
    return (
        products.where(F.col("category").isNotNull())
        .groupBy("serial_no", "segment", "category")
        .agg(F.countDistinct("url").alias("product_pages"), F.min("url").alias("example_url"))
        .join(company.select("serial_no", "company_name", "brand"), "serial_no")
        .select("serial_no", "company_name", "brand", "segment", "category", "product_pages", "example_url")
    )


def build_electric_products(company, pages, products):
    headings = pages.select("url", "h1")
    last_segment = F.regexp_replace(F.element_at(F.split("relative_path", "/"), -1), r"\.html?$", "")
    # Drop trailing internal codes mixing letters and digits ("...-greens-mower-ntu1m0e").
    slug = F.regexp_replace(last_segment, r"-(?=[a-z0-9]*[0-9])(?=[a-z0-9]*[a-z])[a-z0-9]{6,}$", "")
    name = clean(F.coalesce(
        F.regexp_replace("h1", r"\s*[|].*$", ""),
        F.initcap(F.regexp_replace(slug, "[-_]+", " "))))
    electric = (
        products.join(headings, "url", "left")
        .where(F.col("relative_path").rlike(ELECTRIC) | F.coalesce(F.col("h1"), F.lit("")).rlike(r"(?i)\b(electric|battery)\b"))
        .where(~last_segment.rlike(ELECTRIC_LANDING))
        .select("serial_no", name.alias("product_name"),
                F.coalesce("category", F.lit("Other")).alias("category"),
                F.coalesce("segment", F.lit("Other")).alias("segment"),
                "url",
                F.when(F.col("h1").isNotNull(), "page heading").otherwise("page address").alias("name_source"))
    )
    first = Window.partitionBy("serial_no", F.lower("product_name")).orderBy("url")
    return (
        electric.withColumn("n", F.row_number().over(first)).where("n = 1").drop("n")
        .join(company.select("serial_no", "company_name", "brand"), "serial_no")
        .select("serial_no", "company_name", "brand", "product_name", "segment", "category", "url", "name_source")
    )


def build_tech_mentions(spark, company, pages):
    text_pages = pages.where(F.col("word_count") >= MIN_WORDS)
    counts = text_pages.groupBy("serial_no").agg(
        F.count("*").alias("text_pages"),
        *[F.sum(F.col("body_text").rlike(rx).cast("int")).alias(f"t{i}")
          for i, rx in enumerate(TECH_THEMES.values())])
    themes = spark.createDataFrame([(i, t) for i, t in enumerate(TECH_THEMES)], "theme_id int, theme string")
    mentions = reduce(lambda a, b: a.unionByName(b), [
        counts.select("serial_no", F.lit(i).alias("theme_id"), F.col(f"t{i}").alias("pages_mentioning"))
        for i in range(len(TECH_THEMES))])
    has_data = F.coalesce(F.col("text_pages"), F.lit(0)) >= MIN_TEXT_PAGES
    return (
        company.select("serial_no", "company_name", "brand").crossJoin(themes)
        .join(counts.select("serial_no", "text_pages"), "serial_no", "left")
        .join(mentions, ["serial_no", "theme_id"], "left")
        .select(
            "serial_no", "company_name", "brand", "theme_id", "theme",
            F.coalesce("text_pages", F.lit(0)).alias("text_pages"),
            F.when(has_data, F.col("pages_mentioning")).alias("pages_mentioning"),
            F.when(has_data, F.round(100 * F.col("pages_mentioning") / F.col("text_pages"), 1)).alias("pct_pages"),
            has_data.alias("has_data"),
        )
    )


def build_news_by_year(spark, company):
    year = F.regexp_extract("relative_path", r"(?:^|/)((?:19|20)\d{2})(?:/|-|$)", 1)
    news = (
        spark.table(f"{SILVER}.site_urls")
        .where(F.col("page_type") == "news")
        .withColumn("year", F.when(year != "", year.cast("int")))
        .where(F.col("year").isin(NEWS_YEARS))
        .withColumn("serial_no", F.col("serial_no").cast("int"))
        .groupBy("serial_no", "year").agg(F.countDistinct("url").alias("news_pages"))
    )
    years = spark.createDataFrame([(y,) for y in NEWS_YEARS], "year int")
    # Every year for every company with dated news, so lines show zero years too.
    return (
        news.select("serial_no").distinct().crossJoin(years)
        .join(news, ["serial_no", "year"], "left")
        .join(company.select("serial_no", "company_name", "brand"), "serial_no")
        .select("serial_no", "company_name", "brand", "year", F.coalesce("news_pages", F.lit(0)).alias("news_pages"))
    )


def main():
    spark = SparkSession.builder.getOrCreate()
    company = companies(spark)
    pages = clean_pages(spark)
    products = product_urls(spark)

    save(build_dim_company(spark, company, pages, products), "dim_company")
    save(build_company_categories(company, products), "company_categories")
    save(build_electric_products(company, pages, products), "electric_products")
    save(build_tech_mentions(spark, company, pages), "tech_mentions")
    save(build_news_by_year(spark, company), "news_by_year")


if __name__ == "__main__":
    main()

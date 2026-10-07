"""
Step 8 - Silver stage: company contact details.

Picks one address, one phone number and a founded year per company from what
scrape_sites.py saved in silver.site_pages, looks up map coordinates for the
address with OpenStreetMap's Nominatim service and writes
silver.company_contacts. Visits no company websites.

Address choice, best first:
  1. preceded by words like "headquarters" or "corporate office"
  2. found in structured data
  3. found on a contact, locations or about page
  then, between equals, the one found on the most pages.
Phone choice: the number printed right after the chosen address; otherwise
the same order as for addresses, with phone links counting like structured data.
Founded year: the earliest "since / founded / established <year>" on the home
and about pages, only where the website speaks for itself ("we", "our") or in
the homepage title and description; as stated by the website, not checked.
"""
import re
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BRONZE_TABLE = "equipmentcompanies.bronze.equipment_companies"
PAGES_TABLE = "equipmentcompanies.silver.site_pages"
CONTACTS_TABLE = "equipmentcompanies.silver.company_contacts"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_DELAY_SECONDS = 1.1  # usage policy: at most one request per second
USER_AGENT = "EquipmentDashboardBot/1.0 (+https://github.com/Venkat2701/DataBricks)"
NOT_FOUND = "not found on website"

HQ_RE = re.compile(r"(?i)head\s*quarters?|head office|corporate (?:office|offices|hq|campus)|main office|global hq|\bhq\b")
CONTACT_URL_RE = re.compile(r"(?i)contact|location|headquarter|head-office|about|corporate|our-company")
FAX_RE = re.compile(r"(?i)fax")
PHONE_IN_TEXT_RE = re.compile(r"(?<![\d-])(?:\+?1[\s.-])?(?:\(1\)\s*)?\(?([2-9]\d{2})\)?[\s.-]{1,2}([2-9]\d{2})[\s.-](\d{4})(?![\d-])")
FOUNDED_RE = re.compile(r"(?i)\b(?:since|founded(?:\s+in)?|established(?:\s+in)?|est\.)\s+((?:18|19|20)\d{2})\b")
FIRST_PERSON_RE = re.compile(r"\b(?:[Ww]e|[Oo]ur|us)\b")  # lowercase "us" only: "US" is the country
# Sentences about a site, award or partner, not the company's own founding.
NOT_FOUNDING_RE = re.compile(r"(?i)facilit|plant|factory|site|campus|office|center|centre|location|dealer|"
                             r"award|program|partner|acquired|employee|career|ceo|president")
ADDRESS_RE = re.compile(r"^(?P<city>.*?),\s*(?P<state>[A-Z]{2})\s+(?P<zip>\d{5})$")
# Scraped "city" text can start with the street ("River Green Parkway Duluth");
# everything up to the last of these words is dropped.
STREET_WORDS = {"way", "parkway", "pkwy", "drive", "dr", "road", "rd", "street", "st", "avenue", "ave",
                "boulevard", "blvd", "lane", "ln", "court", "ct", "place", "pl", "circle", "highway", "hwy",
                "suite", "ste", "corporation", "corp", "llc", "inc", "company", "usa"}
DIRECTIONS = {"n", "s", "e", "w", "ne", "nw", "se", "sw"}

CONTACTS_SCHEMA = T.StructType([
    T.StructField("serial_no", T.StringType()),
    T.StructField("company_name", T.StringType()),
    T.StructField("hq_address", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("zip", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("address_match", T.StringType()),
    T.StructField("address_source_url", T.StringType()),
    T.StructField("address_context", T.StringType()),
    T.StructField("phone", T.StringType()),
    T.StructField("phone_match", T.StringType()),
    T.StructField("phone_source_url", T.StringType()),
    T.StructField("phone_context", T.StringType()),
    T.StructField("founded_year", T.IntegerType()),
    T.StructField("founded_source_url", T.StringType()),
    T.StructField("geocoded_place", T.StringType()),
    T.StructField("geocoded_city", T.StringType()),
    T.StructField("updated_at", T.TimestampType()),
])


def text_before(context):
    """Text in front of the [[match]]; "headquarters" after an address is about something else."""
    return context.split("[[")[0]


def address_score(context, source, url):
    if HQ_RE.search(text_before(context)):
        return 3, "stated headquarters"
    if source == "json_ld":
        return 2, "structured data"
    if CONTACT_URL_RE.search(url):
        return 1, "contact or about page"
    return 0, "most frequent on site"


def phone_score(context, source, url):
    if FAX_RE.search(text_before(context)[-25:]):
        return None  # a fax number
    if HQ_RE.search(text_before(context)):
        return 3, "stated headquarters"
    if source in ("tel_link", "json_ld"):
        return 2, "phone link" if source == "tel_link" else "structured data"
    if CONTACT_URL_RE.search(url):
        return 1, "contact or about page"
    return 0, "most frequent on site"


def pick(candidates, score_fn):
    """Best of (value, context, source, url) candidates: highest score, then most pages."""
    best = defaultdict(lambda: {"score": -1, "pages": set()})
    for value, context, source, url in candidates:
        scored = score_fn(context or "", source, url)
        if scored is None:
            continue
        entry = best[value]
        entry["pages"].add(url)
        if scored[0] > entry["score"]:
            entry.update(score=scored[0], match=scored[1], value=value, url=url, context=context)
    return max(best.values(), key=lambda e: (e["score"], len(e["pages"])), default=None)


def phone_after(context):
    """First non-fax phone number printed right after an address [[match]], formatted."""
    after = context.split("]]", 1)[1] if "]]" in context else ""
    for m in PHONE_IN_TEXT_RE.finditer(after):
        if not FAX_RE.search(after[max(0, m.start() - 12):m.start()]):
            return "-".join(m.groups())
    return None


def clean_city(text):
    """'River Green Parkway Duluth' -> 'Duluth', 'St. N. Wahpeton' -> 'Wahpeton'."""
    words = text.split()
    cut = max((i for i, w in enumerate(words) if w.strip(".,").lower() in STREET_WORDS), default=-1)
    words = words[cut + 1:]
    while words and words[0].strip(".,").lower() in DIRECTIONS:
        words = words[1:]
    return " ".join(words)


def founded(pages):
    """Earliest founding year the website states about itself, and its page."""
    years = []
    for p in pages:
        if p.page_type == "home":
            for text in (p.title, p.meta_description):
                years += [(int(y), p.url) for y in FOUNDED_RE.findall(text or "")]
        if p.page_type in ("home", "about"):
            text = " ".join(filter(None, [p.meta_description, p.body_text]))
            for sentence in re.split(r"(?<=[.!?])\s+", text):
                if FIRST_PERSON_RE.search(sentence) and not NOT_FOUNDING_RE.search(sentence):
                    years += [(int(y), p.url) for y in FOUNDED_RE.findall(sentence)]
    return min(((y, u) for y, u in years if 1800 <= y <= datetime.now().year), default=(None, None))


def geocode(session, zip_code, state):
    """(latitude, longitude, city, place name) for a US ZIP code, or None."""
    time.sleep(NOMINATIM_DELAY_SECONDS)
    resp = session.get(NOMINATIM_URL, timeout=30, params={
        "postalcode": zip_code, "country": "United States",
        "format": "jsonv2", "addressdetails": 1, "limit": 1})
    resp.raise_for_status()
    results = resp.json()
    if not results:
        return None
    place, address = results[0], results[0].get("address", {})
    if address.get("ISO3166-2-lvl4", f"US-{state}").upper() != f"US-{state}":
        return None  # ZIP belongs to another state; the scraped address is suspect
    city = next((address[k] for k in ("city", "town", "village", "hamlet", "municipality")
                 if address.get(k)), None)
    return float(place["lat"]), float(place["lon"]), city, place.get("display_name")


def previous_locations(spark):
    """Coordinates from the last run, so unchanged addresses are not looked up again."""
    try:
        if not spark.catalog.tableExists(CONTACTS_TABLE):
            return {}
    except Exception:  # catalog lookups are not essential; just geocode again
        return {}
    table = spark.table(CONTACTS_TABLE)
    if "geocoded_city" not in table.columns:  # saved before that column existed
        return {}
    rows = table.where("latitude IS NOT NULL").collect()
    return {(r.state, r.zip): (r.latitude, r.longitude, r.geocoded_city, r.geocoded_place) for r in rows}


def main():
    spark = SparkSession.builder.getOrCreate()
    companies = spark.table(BRONZE_TABLE).select("serial_no", "company_name").collect()
    pages = (
        spark.table(PAGES_TABLE)
        .select("serial_no", "url", "page_type", "title", "meta_description",
                F.when(F.col("page_type").isin("home", "about"), F.col("body_text")).alias("body_text"),
                "contact_phones", "contact_addresses")
        .collect()
    )
    pages_by_company = defaultdict(list)
    for p in pages:
        pages_by_company[p.serial_no].append(p)

    cache = previous_locations(spark)
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    rows = []
    for company in companies:
        company_pages = pages_by_company[company.serial_no]
        address = pick(((a.value, a.context, a.source, p.url)
                        for p in company_pages for a in (p.contact_addresses or [])
                        if ADDRESS_RE.match(a.value)), address_score)
        phone = pick(((c.value, c.context, c.source, p.url)
                      for p in company_pages for c in (p.contact_phones or [])), phone_score)
        next_phone = phone_after(address["context"]) if address else None
        if next_phone:
            phone = {"value": next_phone, "match": "next to address",
                     "url": address["url"], "context": address["context"]}
        year, year_url = founded(company_pages)

        row = {"serial_no": company.serial_no, "company_name": company.company_name,
               "address_match": NOT_FOUND, "phone_match": NOT_FOUND,
               "founded_year": year, "founded_source_url": year_url,
               "updated_at": datetime.now(timezone.utc)}
        if address:
            parsed = ADDRESS_RE.match(address["value"])
            state, zip_code = parsed["state"], parsed["zip"]
            location = cache.get((state, zip_code))
            if location is None:
                try:
                    location = geocode(session, zip_code, state)
                except (requests.RequestException, ValueError) as e:
                    print(f"Geocoding failed for {company.company_name} ({state} {zip_code}): {e}")
            latitude, longitude, geo_city, place = location or (None, None, None, None)
            # The website's own city name; the ZIP lookup's city is only a fallback
            # (one ZIP can cover several towns). If the scraped name is the looked-up
            # city plus a leftover word from the street ("Lyndale Avenue South,
            # Bloomington" -> "South Bloomington"), the looked-up city wins.
            city = clean_city(parsed["city"]) or geo_city or parsed["city"]
            if geo_city and city != geo_city and city.endswith(" " + geo_city):
                city = geo_city
            row.update(hq_address=f"{city}, {state} {zip_code}", city=city, state=state, zip=zip_code,
                       latitude=latitude, longitude=longitude, geocoded_place=place, geocoded_city=geo_city,
                       address_match=address["match"], address_source_url=address["url"],
                       address_context=address["context"])
        if phone:
            row.update(phone=phone["value"], phone_match=phone["match"],
                       phone_source_url=phone["url"], phone_context=phone["context"])
        rows.append(row)
        print(f"{company.serial_no:>3} {company.company_name:<32} "
              f"{row.get('hq_address') or '-':<28} {row.get('phone') or '-':<13} "
              f"founded={year or '-'} coords={'yes' if row.get('latitude') else 'no'} "
              f"[{row['address_match']} / {row['phone_match']}]")

    data = [tuple(r.get(f.name) for f in CONTACTS_SCHEMA.fields) for r in rows]
    (
        spark.createDataFrame(data, CONTACTS_SCHEMA).write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(CONTACTS_TABLE)
    )
    print(f"Wrote {len(data)} rows to {CONTACTS_TABLE}")


if __name__ == "__main__":
    main()

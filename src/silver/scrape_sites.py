"""
Step 8 - Silver stage.

Visits each company website listed in the bronze table and writes three tables:
  silver.site_urls      every US English page address found in the site's sitemap
                        (or by following links when a site has no sitemap)
  silver.site_pages     content of up to MAX_PAGES_PER_SITE key pages per company:
                        the start page, then contact/about pages, then product
                        pages, then the rest; plus the phone numbers and US
                        addresses found anywhere on each page (header and
                        footer included), for company_contacts.py
  silver.scrape_status  one row per company describing how the scrape went

Follows robots.txt, waits between requests to the same site and does not try to
get around blocked or captcha pages. Site-specific settings are in site_rules.py.
Tables are replaced on every run.
"""
import gzip
import heapq
import inspect
import os
import re
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from html import unescape
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql import types as T

# Databricks runs script tasks with exec(), so __file__ is not set; the code
# object still knows the script's path. Needed to import site_rules.py.
sys.path.insert(0, os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))
from site_rules import DEFAULT_RULE, SITE_RULES  # noqa: E402

BRONZE_TABLE = "equipmentcompanies.bronze.equipment_companies"
SILVER_SCHEMA = "equipmentcompanies.silver"

MAX_PAGES_PER_SITE = 100
MAX_PRODUCT_PAGES = 80
MAX_CONTACT_PAGES = 10
MAX_SITEMAP_FILES = 150
SITE_TIME_LIMIT_SECONDS = 20 * 60
REQUEST_TIMEOUT_SECONDS = 20
MIN_DELAY_SECONDS = 1.0
MAX_DELAY_SECONDS = 10.0
MAX_CONSECUTIVE_FAILURES = 5
EMPTY_PAGE_WORDS = 20
EMPTY_PAGES_BEFORE_SKIP = 10
MAX_TEXT_CHARS = 50_000
MAX_CONTACTS_PER_PAGE = 20
CONTACT_CONTEXT_CHARS = 80
MAX_JSON_LD_BLOCKS = 3
MAX_JSON_LD_CHARS = 10_000
JS_RENDERED_MEDIAN_WORDS = 50

BOT_NAME = "EquipmentDashboardBot"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 "
    f"{BOT_NAME}/1.0 (+https://github.com/Venkat2701/DataBricks)"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

LOC_RE = re.compile(r"<(?:\w+:)?loc>\s*(?:<!\[CDATA\[)?\s*(.*?)\s*(?:\]\]>)?\s*</(?:\w+:)?loc>", re.S)
LASTMOD_RE = re.compile(r"<(?:\w+:)?lastmod>\s*(.*?)\s*</(?:\w+:)?lastmod>", re.S)
URL_BLOCK_RE = re.compile(r"<(?:\w+:)?url>(.*?)</(?:\w+:)?url>", re.S)
DOCUMENT_RE = re.compile(r"\.(pdf|jpe?g|png|gif|svg|webp|zip|docx?|xlsx?|pptx?|mp4|mp3)$")
# "(?<!re)" so ordinary pages mentioning a reCAPTCHA-protected form are not flagged.
CAPTCHA_RE = re.compile(r"(?<!re)captcha|are you a robot|verify you are human|access denied|request unsuccessful", re.I)

# Pages that usually carry the company address and main phone number.
CONTACT_PAGE_RE = re.compile(r"(?:^|[/_-])(contact\w*|locations?|headquarters?|head-office|corporate\w*|about\w*|who-we-are|our-company)(?:[/_.-]|$)")
CONTACT_PAGE_FIRST_RE = re.compile(r"contact|location|headquarter|head-office")
NOT_CONTACT_PAGE_RE = re.compile(r"dealer|careers?|jobs?|news|press|blog")

US_STATES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "District of Columbia": "DC",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
    "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA",
    "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR",
    "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA",
    "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}
_STATE_NAMES = "|".join(sorted(map(re.escape, US_STATES), key=len, reverse=True))
_STATE_CODES = "|".join(sorted(set(US_STATES.values())))
# "City, ST 12345", "City, State 12345" or "City, State, 12345", with up to four
# capitalized words before the comma (street words are removed later).
US_ADDRESS_RE = re.compile(
    r"\b([A-Z][A-Za-z.'-]*(?:\s+[A-Z][A-Za-z.'-]*){0,3}),?\s+"
    rf"({_STATE_NAMES}|{_STATE_CODES})[.,]?\s+(\d{{5}})(?:-\d{{4}})?\b")
# US phone numbers in text need separators, so part numbers and IDs are not picked up.
PHONE_RE = re.compile(r"(?<![\d-])(?:\+?1[\s.-])?\(?([2-9]\d{2})\)?[\s.-]{1,2}([2-9]\d{2})[\s.-](\d{4})(?![\d-])")
JSON_LD_PHONE_RE = re.compile(r'"telephone"\s*:\s*"([^"]+)"')
JSON_LD_POSTAL_RE = re.compile(r'"postalCode"\s*:\s*"(\d{5})')
JSON_LD_REGION_RE = re.compile(r'"addressRegion"\s*:\s*"([^"]+)"')
JSON_LD_LOCALITY_RE = re.compile(r'"addressLocality"\s*:\s*"([^"]+)"')


def _tokens(*words):
    # Whole words inside a URL path, where "/", "-", "_" and "." separate words.
    return re.compile(r"(?:^|[/_.-])(?:" + "|".join(words) + r")(?:$|[/_.-])")


# Checked in order; the first match wins. "product" is only used for sites
# without a product_pattern in site_rules.py.
PAGE_TYPE_PATTERNS = [
    ("news", _tokens(r"news\w*", "press", r"blogs?", "media", "stories", r"articles?",
                     r"publications?", r"insights?", "magazine", r"events?")),
    ("careers", _tokens(r"careers?", r"jobs?")),
    ("dealer", _tokens(r"dealers?", "locator")),
    ("parts_service", _tokens("parts", r"service\w*", "support", "warranty", r"manuals?",
                              "training", "safety", "operator-reference")),
    ("about", _tokens(r"about\w*", "company", "history", "sustainability", r"investors?",
                      "leadership", r"contact\w*")),
    ("product", _tokens(r"products?", "equipment", r"machines?", "machinery", r"attachments?",
                        "series", r"models?", r"tractors?", r"excavators?", r"loaders?",
                        r"backhoes?", r"dozers?", r"mowers?", r"harvesters?", r"combines?",
                        r"balers?", r"sprayers?", r"planters?", r"telehandlers?",
                        r"compactors?", r"rollers?", r"trucks?", r"cranes?", r"lifts?",
                        r"drills?", r"trenchers?", r"grinders?", r"chippers?", r"rakes?",
                        r"mixers?", r"pavers?", r"graders?")),
]
# Order in which pages are picked for scraping after the start page.
PAGE_PRIORITY = {"product": 0, "about": 1, "parts_service": 2, "other": 3, "news": 4,
                 "dealer": 5, "careers": 6}

URL_SCHEMA = T.StructType([
    T.StructField("serial_no", T.StringType()),
    T.StructField("company_name", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("relative_path", T.StringType()),
    T.StructField("page_type", T.StringType()),
    T.StructField("section", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("depth", T.IntegerType()),
    T.StructField("lastmod", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("discovered_at", T.TimestampType()),
])

# A phone number or address found on a page. source: text, tel_link or json_ld.
CONTACT_SCHEMA = T.StructType([
    T.StructField("value", T.StringType()),
    T.StructField("context", T.StringType()),
    T.StructField("source", T.StringType()),
])

PAGE_SCHEMA = T.StructType([
    T.StructField("serial_no", T.StringType()),
    T.StructField("company_name", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("final_url", T.StringType()),
    T.StructField("page_type", T.StringType()),
    T.StructField("section", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("http_status", T.IntegerType()),
    T.StructField("fetch_status", T.StringType()),
    T.StructField("error", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("meta_description", T.StringType()),
    T.StructField("h1", T.StringType()),
    T.StructField("headings", T.ArrayType(T.StringType())),
    T.StructField("body_text", T.StringType()),
    T.StructField("word_count", T.IntegerType()),
    T.StructField("json_ld", T.ArrayType(T.StringType())),
    T.StructField("contact_phones", T.ArrayType(CONTACT_SCHEMA)),
    T.StructField("contact_addresses", T.ArrayType(CONTACT_SCHEMA)),
    T.StructField("internal_link_count", T.IntegerType()),
    T.StructField("scraped_at", T.TimestampType()),
])

STATUS_SCHEMA = T.StructType([
    T.StructField("serial_no", T.StringType()),
    T.StructField("company_name", T.StringType()),
    T.StructField("base_url", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("message", T.StringType()),
    T.StructField("robots_http_status", T.IntegerType()),
    T.StructField("sitemap_files_read", T.IntegerType()),
    T.StructField("sitemap_errors", T.IntegerType()),
    T.StructField("urls_found", T.IntegerType()),
    T.StructField("pages_attempted", T.IntegerType()),
    T.StructField("pages_ok", T.IntegerType()),
    T.StructField("pages_failed", T.IntegerType()),
    T.StructField("robots_skipped", T.IntegerType()),
    T.StructField("median_word_count", T.IntegerType()),
    T.StructField("started_at", T.TimestampType()),
    T.StructField("finished_at", T.TimestampType()),
    T.StructField("duration_seconds", T.DoubleType()),
])


class Fetcher:
    """HTTP session for one site that waits `delay` seconds between requests."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.delay = MIN_DELAY_SECONDS
        self._last = 0.0

    def get(self, url):
        wait = self._last + self.delay - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        try:
            return self.session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        finally:
            self._last = time.monotonic()


def now():
    return datetime.now(timezone.utc)


def clean_url(url):
    """Lowercase scheme and host; drop query string and fragment."""
    p = urlparse(unescape(url.strip()))
    return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path or "/", "", "", ""))


def url_key(url):
    return clean_url(url).rstrip("/").lower()


def relative_path(url, base_url):
    """Path below base_url, or None when the URL is outside it."""
    u, b = url_key(url), base_url.rstrip("/").lower()
    if u == b:
        return ""
    if u.startswith(b + "/"):
        return u[len(b) + 1:]
    return None


def describe(rel_path, rule):
    """page_type, section, category and depth of a page from its path."""
    segments = [re.sub(r"\.html?$", "", s) for s in rel_path.split("/") if s]
    section = segments[0] if segments else ""
    if DOCUMENT_RE.search(rel_path):
        page_type = "document"
    elif not segments:
        page_type = "home"
    elif rule["product_pattern"] and re.search(rule["product_pattern"], rel_path):
        page_type = "product"
    else:
        page_type = "other"
        for name, pattern in PAGE_TYPE_PATTERNS:
            if name == "product" and rule["product_pattern"]:
                continue
            if pattern.search(rel_path):
                page_type = name
                break
    i = rule["category_segment"]
    category = segments[i] if page_type == "product" and len(segments) > i else None
    return page_type, section, category, len(segments)


def load_robots(fetcher, base_url):
    p = urlparse(base_url)
    robots = RobotFileParser(f"{p.scheme}://{p.netloc}/robots.txt")
    try:
        resp = fetcher.get(robots.url)
    except requests.RequestException:
        robots.allow_all = True
        return robots, None
    if resp.status_code in (401, 403):
        robots.disallow_all = True
    elif resp.status_code >= 400:
        robots.allow_all = True
    else:
        robots.parse(resp.text.splitlines())
    robots.modified()
    return robots, resp.status_code


def read_sitemaps(fetcher, sitemap_urls, deadline):
    """Returns ({page url: lastmod}, files read, errors). Follows sitemap indexes."""
    queue, seen, entries, files, errors = list(sitemap_urls), set(), {}, 0, 0
    while queue and files < MAX_SITEMAP_FILES and time.monotonic() < deadline:
        sitemap_url = unescape(queue.pop(0))
        if sitemap_url in seen:
            continue
        seen.add(sitemap_url)
        try:
            resp = fetcher.get(sitemap_url)
            resp.raise_for_status()
            body = resp.content
            if body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
        except (requests.RequestException, OSError):
            errors += 1
            continue
        files += 1
        xml = body.decode("utf-8", "replace")
        if "<sitemapindex" in xml:
            queue.extend(LOC_RE.findall(xml))
            continue
        if "<urlset" not in xml:
            # Plain-text sitemap: one address per line (allowed by the sitemap standard).
            for line in xml.splitlines():
                if line.strip().startswith("http"):
                    entries.setdefault(line.strip(), None)
            continue
        for block in URL_BLOCK_RE.findall(xml):
            loc = LOC_RE.search(block)
            if loc:
                lastmod = LASTMOD_RE.search(block)
                entries.setdefault(unescape(loc.group(1)), lastmod.group(1) if lastmod else None)
    return entries, files, errors


def clip(text, limit):
    text = re.sub(r"\s+", " ", text or "").strip()
    return text[:limit] if text else None


def format_phone(digits):
    digits = re.sub(r"\D", "", digits)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10 or digits[0] in "01" or digits[3] in "01":
        return None
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"


def find_contacts(text, links, json_ld):
    """Phone numbers and US addresses on a page, each with some surrounding text."""
    phones, addresses = {}, {}

    def context(m):
        # The match is wrapped in [[ ]] so readers can tell it apart from its surroundings.
        before = text[max(0, m.start() - CONTACT_CONTEXT_CHARS):m.start()]
        return f"{before}[[{m.group(0)}]]{text[m.end():m.end() + CONTACT_CONTEXT_CHARS]}"

    for m in PHONE_RE.finditer(text):
        phone = format_phone(m.group(0))
        if phone:
            phones.setdefault(phone, (phone, context(m), "text"))
    for link in links:
        if link.lower().startswith("tel:"):
            phone = format_phone(link[4:])
            if phone:
                phones.setdefault(phone, (phone, link, "tel_link"))
    for m in US_ADDRESS_RE.finditer(text):
        city, state, zip_code = m.group(1), US_STATES.get(m.group(2), m.group(2)), m.group(3)
        value = f"{city}, {state} {zip_code}"
        addresses.setdefault(value, (value, context(m), "text"))
    for block in json_ld:
        for raw in JSON_LD_PHONE_RE.findall(block):
            phone = format_phone(raw)
            if phone:
                phones.setdefault(phone, (phone, clip(block, 300), "json_ld"))
        postal, region = JSON_LD_POSTAL_RE.search(block), JSON_LD_REGION_RE.search(block)
        if postal and region:
            locality = JSON_LD_LOCALITY_RE.search(block)
            state = US_STATES.get(region.group(1), region.group(1))
            value = f"{locality.group(1) if locality else ''}, {state} {postal.group(1)}".lstrip(", ")
            addresses.setdefault(value, (value, clip(block, 300), "json_ld"))
    return list(phones.values())[:MAX_CONTACTS_PER_PAGE], list(addresses.values())[:MAX_CONTACTS_PER_PAGE]


def scrape_page(fetcher, url):
    page = {"url": url, "final_url": None, "http_status": None, "fetch_status": "error",
            "error": None, "title": None, "meta_description": None, "h1": None,
            "headings": [], "body_text": None, "word_count": 0, "json_ld": [],
            "contact_phones": [], "contact_addresses": [], "links": []}
    try:
        resp = fetcher.get(url)
    except requests.RequestException as e:
        page["error"] = f"{type(e).__name__}: {e}"[:300]
        return page
    page["final_url"], page["http_status"] = resp.url, resp.status_code
    if resp.status_code in (401, 403, 429):
        page["fetch_status"] = "blocked"
        return page
    if resp.status_code >= 400:
        page["fetch_status"] = "http_error"
        return page
    content_type = resp.headers.get("Content-Type", "").lower()
    if "html" not in content_type:
        page["fetch_status"] = "not_html"
        return page

    encoding = resp.encoding if "charset=" in content_type else None
    soup = BeautifulSoup(resp.content, "html.parser", from_encoding=encoding)
    page["title"] = clip(soup.title.get_text(" ") if soup.title else None, 500)
    meta = (soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
            or soup.find("meta", attrs={"property": "og:description"}))
    page["meta_description"] = clip(meta.get("content") if meta else None, 1000)
    h1 = soup.find("h1")
    page["h1"] = clip(h1.get_text(" ") if h1 else None, 500)
    json_ld = [j for j in (clip(s.get_text(), MAX_JSON_LD_CHARS)
                           for s in soup.find_all("script", attrs={"type": "application/ld+json"})) if j]
    page["json_ld"] = json_ld[:MAX_JSON_LD_BLOCKS]
    page["links"] = [urljoin(resp.url, a["href"]) for a in soup.find_all("a", href=True)]

    # Contact details usually sit in the header or footer, so look for them in
    # all visible text before those parts are removed below.
    for tag in soup(["script", "style", "noscript", "svg", "template"]):
        tag.decompose()
    page["contact_phones"], page["contact_addresses"] = find_contacts(
        clip(soup.get_text(" "), 10**7) or "", page["links"], json_ld)

    # Keep the main content only: drop menus, headers, footers and forms.
    for tag in soup(["iframe", "nav", "header", "footer", "form"]):
        tag.decompose()
    root = soup.find("main") or soup.body or soup
    page["headings"] = [h for h in (clip(e.get_text(" "), 300) for e in root.find_all(["h2", "h3"])) if h][:50]
    text = clip(root.get_text(" "), 10**7) or ""
    page["word_count"] = len(text.split())
    page["body_text"] = text[:MAX_TEXT_CHARS] or None

    if CAPTCHA_RE.search(page["title"] or "") or (page["word_count"] < 300 and CAPTCHA_RE.search(text)):
        page["fetch_status"] = "captcha"
    else:
        page["fetch_status"] = "ok"
    return page


def scrape_site(company):
    started, t0 = now(), time.monotonic()
    deadline = t0 + SITE_TIME_LIMIT_SECONDS
    host = urlparse(company.company_site_url).netloc.lower().removeprefix("www.")
    rule = {**DEFAULT_RULE, **SITE_RULES.get(host, {})}
    base_url = rule["base_url"] or company.company_site_url
    start_url = rule["start_url"] or base_url
    exclude = re.compile(rule["exclude"]) if rule["exclude"] else None
    ident = {"serial_no": company.serial_no, "company_name": company.company_name}
    status = {**ident, "base_url": base_url, "status": None, "message": None,
              "robots_http_status": None, "sitemap_files_read": 0, "sitemap_errors": 0,
              "urls_found": 0, "pages_attempted": 0, "pages_ok": 0, "pages_failed": 0,
              "robots_skipped": 0, "median_word_count": None, "started_at": started}
    urls, pages = {}, []

    def finish(state, message=None):
        ok = [p["word_count"] for p in pages if p["fetch_status"] == "ok"]
        status.update(
            status=state, message=message, urls_found=len(urls), pages_attempted=len(pages),
            pages_ok=len(ok), pages_failed=len(pages) - len(ok),
            median_word_count=int(statistics.median(ok)) if ok else None,
            finished_at=now(), duration_seconds=round(time.monotonic() - t0, 1))
        return {"status": status, "urls": list(urls.values()), "pages": pages}

    def add_url(url, lastmod, source):
        rel = relative_path(url, base_url)
        if rel is None or (exclude and exclude.search(rel)):
            return None
        key = url_key(url)
        if key not in urls:
            page_type, section, category, depth = describe(rel, rule)
            urls[key] = {**ident, "url": clean_url(url), "relative_path": rel,
                         "page_type": page_type, "section": section, "category": category,
                         "depth": depth, "lastmod": lastmod, "source": source,
                         "discovered_at": now()}
            return urls[key]
        return None

    def record(url_row, page):
        pages.append({**ident, "page_type": url_row["page_type"], "section": url_row["section"],
                      "category": url_row["category"], "scraped_at": now(),
                      "internal_link_count": sum(1 for l in page["links"]
                                                 if relative_path(l, base_url) is not None),
                      **{k: v for k, v in page.items() if k != "links"}})

    try:
        fetcher = Fetcher()
        robots, status["robots_http_status"] = load_robots(fetcher, base_url)
        delay = robots.crawl_delay(BOT_NAME)
        if delay:
            fetcher.delay = min(max(float(delay), MIN_DELAY_SECONDS), MAX_DELAY_SECONDS)
        if not robots.can_fetch(BOT_NAME, start_url):
            return finish("blocked", f"robots.txt (HTTP {status['robots_http_status']}) does not allow {start_url}")

        sitemap_urls = rule["sitemaps"] or robots.site_maps() or [urljoin(base_url, "/sitemap.xml")]
        entries, status["sitemap_files_read"], status["sitemap_errors"] = read_sitemaps(fetcher, sitemap_urls, deadline)
        for url, lastmod in entries.items():
            add_url(url, lastmod, "sitemap")
        follow_links = not urls  # no usable sitemap: discover pages from links instead

        start_key = url_key(start_url)
        start_row = add_url(start_url, None, "start") or urls.get(start_key) or {
            **ident, "url": start_url, "page_type": "home", "section": "", "category": None}
        start_row["page_type"] = "home"
        home = scrape_page(fetcher, start_url)
        record(start_row, home)
        if home["fetch_status"] in ("blocked", "captcha"):
            return finish(home["fetch_status"], f"start page returned {home['fetch_status']} (HTTP {home['http_status']})")

        queue = []

        def enqueue(row):
            if row and row["page_type"] in PAGE_PRIORITY:
                heapq.heappush(queue, (PAGE_PRIORITY[row["page_type"]], row["depth"], row["url"]))

        if follow_links:
            for link in home["links"]:
                enqueue(add_url(link, None, "links"))
        else:
            for row in urls.values():
                enqueue(row)

        # Contact, locations and about pages go first: they carry the company's
        # address and main phone number, and product pages would crowd them out.
        contact_rows = sorted(
            (r for r in urls.values()
             if r["page_type"] not in ("home", "document")
             and CONTACT_PAGE_RE.search(r["relative_path"])
             and not NOT_CONTACT_PAGE_RE.search(r["relative_path"])),
            key=lambda r: (0 if CONTACT_PAGE_FIRST_RE.search(r["relative_path"]) else 1, r["depth"], r["url"]),
        )[:MAX_CONTACT_PAGES]

        def next_rows():
            yield from contact_rows
            while queue:  # re-checked each time: following links adds to the queue
                yield urls[url_key(heapq.heappop(queue)[2])]

        visited, products, failures = {start_key}, 0, 0
        empty_streak, skipped_types, notes = {}, [], []
        for row in next_rows():
            if len(pages) >= MAX_PAGES_PER_SITE or time.monotonic() >= deadline:
                break
            url, key, page_type = row["url"], url_key(row["url"]), row["page_type"]
            if (key in visited or page_type in skipped_types
                    or (page_type == "product" and products >= MAX_PRODUCT_PAGES)):
                continue
            visited.add(key)
            if not robots.can_fetch(BOT_NAME, url):
                status["robots_skipped"] += 1
                continue
            page = scrape_page(fetcher, url)
            record(row, page)
            products += page_type == "product"

            # Missing pages (404) are normal in stale sitemaps; only signs of being
            # blocked or of an overloaded site count towards stopping.
            hard_failure = (page["fetch_status"] in ("blocked", "captcha", "error")
                            or (page["http_status"] or 0) >= 500)
            failures = failures + 1 if hard_failure else 0
            if failures >= MAX_CONSECUTIVE_FAILURES:
                return finish("partial", f"stopped after {failures} failed pages in a row")

            # Stop requesting a page type whose pages keep arriving empty (built by JavaScript).
            if page["fetch_status"] == "ok":
                streak = empty_streak.get(page_type, 0) + 1 if page["word_count"] < EMPTY_PAGE_WORDS else 0
                empty_streak[page_type] = streak
                if streak >= EMPTY_PAGES_BEFORE_SKIP:
                    skipped_types.append(page_type)
                    notes.append(f"skipped remaining {page_type} pages after {streak} empty ones")

            if follow_links:
                for link in page["links"]:
                    enqueue(add_url(link, None, "links"))

        if time.monotonic() >= deadline:
            notes.append("site time limit reached")
        ok_words = [p["word_count"] for p in pages if p["fetch_status"] == "ok"]
        if not ok_words:
            reason = home["error"] or f"{home['fetch_status']} (HTTP {home['http_status']})"
            return finish("error", f"no page could be scraped; start page: {reason}"[:500])
        if statistics.median(ok_words) < JS_RENDERED_MEDIAN_WORDS:
            notes.insert(0, "pages have little text; content is likely loaded by JavaScript")
            return finish("js_rendered", "; ".join(notes))
        if len(pages) - len(ok_words) > 0.2 * len(pages):
            notes.insert(0, "more than 20% of pages failed")
            return finish("partial", "; ".join(notes))
        return finish("ok", "; ".join(notes) or None)
    except Exception as e:  # one broken site must not stop the others
        return finish("error", f"{type(e).__name__}: {e}"[:500])


def save(spark, rows, schema, table):
    data = [tuple(row.get(f.name) for f in schema.fields) for row in rows]
    (
        spark.createDataFrame(data, schema).write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    print(f"Wrote {len(data)} rows to {table}")


def main():
    spark = SparkSession.builder.getOrCreate()
    companies = (
        spark.table(BRONZE_TABLE)
        .select("serial_no", "company_name", "company_site_url")
        .collect()
    )
    with ThreadPoolExecutor(max_workers=len(companies)) as pool:
        results = list(pool.map(scrape_site, companies))

    for r in results:
        s = r["status"]
        print(f"{s['serial_no']:>3} {s['company_name']:<32} {s['status']:<12} urls={s['urls_found']:<6} "
              f"pages_ok={s['pages_ok']}/{s['pages_attempted']} median_words={s['median_word_count']} "
              f"{s['duration_seconds']}s {s['message'] or ''}")

    save(spark, [r["status"] for r in results], STATUS_SCHEMA, f"{SILVER_SCHEMA}.scrape_status")
    if sum(r["status"]["pages_ok"] for r in results) == 0:
        raise RuntimeError("No page was scraped from any site; site_urls and site_pages were not changed")
    save(spark, [u for r in results for u in r["urls"]], URL_SCHEMA, f"{SILVER_SCHEMA}.site_urls")
    save(spark, [p for r in results for p in r["pages"]], PAGE_SCHEMA, f"{SILVER_SCHEMA}.site_pages")


if __name__ == "__main__":
    main()

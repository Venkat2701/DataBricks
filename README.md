# Equipment Companies Dashboard

A Databricks pipeline that collects data about 15 US equipment companies and
feeds a dashboard. Data moves through the medallion layers in the
`equipmentcompanies` catalog:

| Layer   | Location                                                   | Contents                                  |
|---------|------------------------------------------------------------|-------------------------------------------|
| Landing | `/Volumes/equipmentcompanies/landing/equipmentcompanies/`  | `US_Equipment_Companies.csv` (uploaded)   |
| Bronze  | `equipmentcompanies.bronze.equipment_companies`            | The CSV as a table, values unchanged      |
| Silver  | `equipmentcompanies.silver.*`                              | Data scraped from each company's website  |
| Gold    | `equipmentcompanies.gold.*`                                | Cleaned, normalized tables for dashboards |

## Repo layout

```
databricks.yml                             Bundle config (workspace, targets)
resources/equipment_pipeline.job.yml       Job definition: one task per step
resources/equipment_dashboard.dashboard.yml Dashboard definition (warehouse, file)
src/bronze/landing_to_bronze.py            Landing CSV -> bronze table
src/silver/scrape_sites.py                 Company websites -> silver tables
src/silver/site_rules.py                   Per-site scraping settings
src/silver/company_contacts.py             Scraped pages -> address, phone, founded year, coordinates
src/gold/build_gold.py                     Silver -> cleaned dashboard tables in gold
src/gold/categories.py                     Standard equipment categories and their matching rules
src/dashboard/equipment_companies.lvdash.json  The Databricks AI/BI dashboard
```

The job runs `bronze_ingest` -> `silver_scrape` -> `silver_contacts` -> `gold_build`.

## Bronze table

| Column             | Description                                 |
|--------------------|---------------------------------------------|
| `serial_no`        | `Serial No` from the CSV                    |
| `company_name`     | `Company Name` from the CSV                 |
| `company_site_url` | `Company Site URL` from the CSV             |
| `_source_file`     | Path of the CSV file the row came from      |
| `_ingested_at`     | Time the row was loaded                     |

All CSV values are stored as strings. The table is fully replaced on every
run, so reruns never create duplicates.

## Silver tables

`src/silver/scrape_sites.py` visits the US English part of each company's
website and writes:

| Table                  | One row per                | Main columns                                                                      |
|------------------------|----------------------------|-----------------------------------------------------------------------------------|
| `silver.site_urls`     | page address found         | `url`, `page_type`, `section`, `category`, `depth`, `lastmod`, `source`           |
| `silver.site_pages`    | page scraped (max 100/company) | `title`, `meta_description`, `h1`, `headings`, `body_text`, `word_count`, `json_ld`, `fetch_status` |
| `silver.scrape_status` | company                    | `status`, `message`, `urls_found`, `pages_ok`, `pages_failed`, `median_word_count` |
| `silver.company_contacts` | company                 | `hq_address`, `phone`, `founded_year`, `latitude`, `longitude`, `*_match`, `*_source_url` |

- Page addresses come from each site's sitemap. Sites without a sitemap are
  explored by following links from the start page.
- `page_type` is one of `home`, `product`, `about`, `parts_service`, `news`,
  `dealer`, `careers`, `document`, `other`, worked out from the address.
- Pages are scraped in this order: start page, contact/locations/about pages
  (max 10), product pages (max 80), then the rest, up to 100 pages per company.
- Phone numbers and US addresses are collected from the whole page, header and
  footer included, into `site_pages.contact_phones` / `contact_addresses`.
  `company_contacts.py` then picks one of each per company: preferably one
  next to "headquarters"/"corporate office", else one from a phone link,
  structured data or a contact/about page, else the one found on most pages.
  `*_match` says which rule picked it and `*_source_url` where it came from.
- Map coordinates come from OpenStreetMap Nominatim, looked up by ZIP code
  (one request per second, only for addresses not looked up before).
- `founded_year` is the earliest "since/founded/established <year>" that the
  website states about itself: in the homepage title or description, or in a
  home/about page sentence using "we"/"our" that is not about a facility,
  award or partner. As stated by the website; not checked.
- `scrape_status.status` is `ok`, `partial`, `js_rendered` (pages have little
  text because they are built by JavaScript), `captcha`, `blocked` or `error`.
- The scraper follows robots.txt, waits at least 1 second between requests to
  the same site, and does not try to get around blocked or captcha pages.
- If 10 pages of one type in a row arrive with almost no text (built by
  JavaScript), the scraper stops requesting that type for that site and moves
  on to other page types. `scrape_status.message` says when this happened.
- Per-site settings (US section of the site, product page addresses, pages to
  skip) are in `src/silver/site_rules.py`. Companies without an entry are
  scraped from their bronze URL with default settings.

## Gold tables

`src/gold/build_gold.py` cleans the silver data and builds one table per
dashboard need. Every table has `company_name` and a short `brand`
("Kubota Tractor Corporation" -> "Kubota") used in charts and filters.

| Table                     | One row per                  | Main columns                                                              |
|---------------------------|------------------------------|---------------------------------------------------------------------------|
| `gold.dim_company`        | company                      | `website`, `tagline`, `hq_address`, `phone`, `latitude`, `longitude`, `founded_year`, `data_note` |
| `gold.company_categories` | company x equipment category | `segment`, `category`, `product_pages`, `example_url`                     |
| `gold.electric_products`  | electric or battery product  | `product_name`, `category`, `segment`, `url`                              |
| `gold.tech_mentions`      | company x technology theme   | `text_pages`, `pages_mentioning`, `pct_pages`, `has_data`                 |
| `gold.news_by_year`       | company x year (last 5)      | `news_pages`                                                              |

Cleaning applied: pages that redirected to another website and pages that
repeat another page's text are dropped; compare/finance/parts/offer pages
listed under product sections are dropped; names are trimmed and normalized.
Each product page is given one of ~25 standard categories (`categories.py`);
the job log lists the most common pages left without a category so new rules
can be added. Technology themes are only counted for companies with at least
20 pages of text (`has_data`).

## Dashboard

**Equipment Companies Dashboard** (Databricks AI/BI, under **Dashboards**)
reads only the gold tables, on the Serverless Starter Warehouse:

1. KPI tiles: companies, HQs found, equipment categories, product pages,
   electric products, news pages this year
2. Headquarters map, with a company directory (HQ, phone, founded, data status)
3. Product portfolio heatmap: company x equipment category
4. Electric and battery products per company
5. Technology focus heatmap: company x theme
6. News activity per year

Filters: Company (all widgets) and Segment (portfolio and electric charts).
The dashboard is defined in `src/dashboard/equipment_companies.lvdash.json`
and deployed with the bundle; edits made in the Databricks UI are overwritten
by the next `bundle deploy` unless copied back into that file.

## Deploy and run

Requires the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html).

```sh
databricks auth login --host https://dbc-c0dead6e-f16f.cloud.databricks.com
databricks bundle deploy
databricks bundle run equipment_companies_pipeline
# rebuild only contacts and gold from the last scrape:
databricks bundle run equipment_companies_pipeline --only "silver_contacts+"
```

The job, `equipment_companies_pipeline`, then appears under
**Jobs & Pipelines** and can also be run from there. It runs on serverless
compute and has no schedule. A full run takes about 15 minutes, mostly the
polite (1 request per second) website scraping.

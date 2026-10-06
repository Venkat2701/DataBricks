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
databricks.yml                         Bundle config (workspace, targets)
resources/equipment_pipeline.job.yml   Job definition: one task per layer
src/bronze/landing_to_bronze.py        Landing CSV -> bronze table
src/silver/scrape_sites.py             Company websites -> silver tables
src/silver/site_rules.py               Per-site scraping settings
```

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

- Page addresses come from each site's sitemap. Sites without a sitemap are
  explored by following links from the start page.
- `page_type` is one of `home`, `product`, `about`, `parts_service`, `news`,
  `dealer`, `careers`, `document`, `other`, worked out from the address.
- Pages are scraped in this order: start page, product pages (max 80), about
  pages, then the rest, up to 100 pages per company.
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

## Deploy and run

Requires the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html).

```sh
databricks auth login --host https://dbc-c0dead6e-f16f.cloud.databricks.com
databricks bundle deploy
databricks bundle run equipment_companies_pipeline
```

The job, `equipment_companies_pipeline`, then appears under
**Jobs & Pipelines** and can also be run from there. It runs on serverless
compute and has no schedule.

# Equipment Companies Dashboard — Project Documentation

| | |
|---|---|
| **Project** | Building an Equipment Companies Dashboard in Databricks |
| **Organization** | Mayvel Technologies Ltd (previously Shloklabs) |
| **Prepared by** | Venkatesh P |
| **Date** | 07/10/2026 |
| **Version** | 1.1 |
| **Repository** | github.com/Venkat2701/DataBricks (branch `main`) |
| **Platform** | Databricks Free Edition (serverless) |

### Revision history

| Version | Date | Changes |
|---|---|---|
| 1.0 | 07/10/2026 | First release: 15 companies, 4-task pipeline, dashboard with 6 visuals |
| 1.1 | 07/10/2026 | 5 companies added (20 in total); clickable Website column in the company directory; automatic dashboard refresh as the job's 5th task; site-settings and category updates (section 5, "Enhancements after the first release") |

---

## Contents

1. Project Overview
2. Target of the Project
3. Pre-requisites Prepared
4. Databricks Medallion Architecture Planned
5. Steps of Building the Dashboard
6. Dashboard Output

---

## 1. Project Overview

This project builds an automated data pipeline and an interactive dashboard in Databricks that compares **20 US equipment manufacturers** (construction, agriculture, turf, forestry, mining, aerial and crane equipment) using the information each company publishes on its own website. The first release covered 15 companies; 5 more were added on 07/10/2026.

A list of the companies and their website addresses is uploaded to Databricks as a CSV file. A Databricks job then:

1. loads the list into a **bronze** table,
2. visits each company's US website, reads its sitemap and scrapes its key pages into **silver** tables,
3. extracts each company's headquarters address, phone number and founded year, and looks up map coordinates,
4. cleans, normalizes and aggregates the data into **gold** tables designed for the dashboard,
5. refreshes the dashboard so it shows the new data,

and a Databricks **AI/BI dashboard** presents the gold tables as six visuals: KPI tiles, a headquarters map with a company directory, a product portfolio heatmap, an electric product lineup, a technology focus heatmap and news activity over time.

Everything — pipeline code, job definition and dashboard — is kept as code in a GitHub repository and deployed to Databricks with a Databricks Asset Bundle.

### Key facts

| Item | Value |
|---|---|
| Companies covered | 20 (US-based equipment manufacturers) |
| Data source | Each company's public US website (sitemaps and pages) |
| Architecture | Medallion: Landing → Bronze → Silver → Gold |
| Catalog | `equipmentcompanies` (Unity Catalog) |
| Orchestration | Databricks job `equipment_companies_pipeline` (5 tasks, ending with a dashboard refresh) |
| Compute | Serverless jobs compute; Serverless Starter Warehouse for SQL and the dashboard |
| Deployment | Databricks Asset Bundle (`databricks bundle deploy`) from the Git repository |
| Dashboard | Databricks AI/BI dashboard "Equipment Companies Dashboard", refreshed automatically by the job |
| Full pipeline run time | About 20 minutes (mostly polite website scraping: at least 1 second between requests per site; one site asks for 10 seconds) |
| Data volume (latest run) | 14,578 page addresses, 1,767 pages scraped, 20 companies × 5 gold tables |

### Technology stack

| Area | Technology |
|---|---|
| Data platform | Databricks Free Edition, Unity Catalog, Delta tables, Volumes |
| Processing | PySpark (Spark Connect on serverless), Spark SQL |
| Scraping | Python `requests`, BeautifulSoup 4, `urllib.robotparser` |
| Geocoding | OpenStreetMap Nominatim (free address lookup service) |
| Orchestration | Databricks Jobs (Lakeflow Jobs) with Python script tasks |
| Deployment | Databricks CLI v1.19.0, Databricks Asset Bundles |
| Visualization | Databricks AI/BI dashboard (Lakeview), defined in `.lvdash.json` |
| Source control | Git, GitHub, Databricks Git folder |
| Development | VS Code on Windows, PowerShell |

---

## 2. Target of the Project

### 2.1 Business goal

Give a quick, visual comparison of the main US equipment manufacturers, built only from what they publish themselves. The dashboard answers five questions:

| Question | Answered by |
|---|---|
| Where are these companies based? | Headquarters map and company directory |
| What does each company make, and who competes with whom? | Product portfolio heatmap |
| Who is leading the move to electric machines? | Electric & battery products chart |
| What does each company emphasize (electric, autonomy, telematics, sustainability…)? | Technology focus heatmap |
| How active is each company in publishing news? | News activity per year |

### 2.2 Technical goals

- Follow the planned workflow (steps 1–10 of the *Equipment Dashboard Workflow* plan dated 06/10/2026).
- Use the Databricks **medallion architecture** with a separate schema per layer.
- Build a **repeatable, automated** pipeline: one job run refreshes every table, and reruns never create duplicates.
- Keep pipeline code, job definition and dashboard **as code** in GitHub and deploy them with one command.
- Collect data **responsibly**: follow each site's `robots.txt`, wait between requests, and never try to get around blocked or captcha-protected pages.
- Keep every value **traceable**: store the source page of scraped facts so they can be checked.

### 2.3 Scope

| In scope | Out of scope |
|---|---|
| The US English section of each company's website | Other countries' and other languages' sections |
| Sitemap inventory of all US pages; content of up to 100 key pages per company | Scraping every page of every site (≈15,000 pages; hours of crawling) |
| Facts published on the websites (address, phone, product pages, news pages) | Manually typed reference data or third-party data sources |
| A current snapshot (tables replaced on each run) | Historical tracking of changes over time |
| Respecting blocks and captchas | Bypassing website protections or browser automation |

### 2.4 Success criteria and status

| Criterion | Status |
|---|---|
| CSV loaded to bronze with all companies | Achieved (20 rows) |
| Websites scraped into silver for most companies | Achieved: 17 full, 1 partial (John Deere, JavaScript site), 2 no data (Caterpillar did not respond; Link-Belt blocks automated access) |
| Gold tables built for every planned visual | Achieved (5 tables) |
| Dashboard with 6 visuals deployed in Databricks | Achieved: deployed, published, displaying data and refreshed automatically by the job |
| Whole solution deployable from GitHub with one command | Achieved (`databricks bundle deploy`) |

---

## 3. Pre-requisites Prepared

### 3.1 Platform and accounts

| Pre-requisite | Details |
|---|---|
| Databricks workspace | Databricks Free Edition workspace `dbc-c0dead6e-f16f.cloud.databricks.com` |
| Compute | Serverless compute for jobs (environment version 2); **Serverless Starter Warehouse** (2X-Small, ID `38dfe64819487879`) for SQL and the dashboard |
| Internet access | Outbound internet access from serverless compute confirmed with a test run (14 of 15 company websites reachable) |
| GitHub repository | `Venkat2701/DataBricks`, branch `main`; the developer's GitHub account was added as a collaborator so it can push |

### 3.2 Unity Catalog objects

| Object | Name | Purpose |
|---|---|---|
| Catalog | `equipmentcompanies` | Holds all project data |
| Schema | `equipmentcompanies.landing` | Raw input files |
| Volume | `equipmentcompanies.landing.equipmentcompanies` | Stores the uploaded CSV (`/Volumes/equipmentcompanies/landing/equipmentcompanies/`) |
| Schema | `equipmentcompanies.bronze` | Raw data as tables |
| Schema | `equipmentcompanies.silver` | Scraped and extracted website data |
| Schema | `equipmentcompanies.gold` | Cleaned, dashboard-ready tables |

### 3.3 Source data: the company list

The list of US equipment companies was prepared with AI assistance (15 companies) and extended with 5 more on 07/10/2026. It is saved as `US_Equipment_Companies.csv` with three columns: **Serial No**, **Company Name** and **Company Site URL**.

| # | Company | Website |
|---|---|---|
| 1 | John Deere | https://www.deere.com/ |
| 2 | Caterpillar | https://www.cat.com/ |
| 3 | Case Construction Equipment | https://www.casece.com/ |
| 4 | Bobcat Company | https://www.bobcat.com/ |
| 5 | Komatsu America | https://www.komatsu.com/ |
| 6 | Kubota Tractor Corporation | https://www.kubotausa.com/ |
| 7 | JCB North America | https://www.jcb.com/ |
| 8 | New Holland Agriculture | https://agriculture.newholland.com/ |
| 9 | AGCO Corporation | https://www.agcocorp.com/ |
| 10 | Terex Corporation | https://www.terex.com/ |
| 11 | Volvo Construction Equipment | https://www.volvoce.com/ |
| 12 | Hitachi Construction Machinery | https://www.hitachicm.us/ |
| 13 | Wacker Neuson | https://www.wackerneuson.com/ |
| 14 | Vermeer Corporation | https://www.vermeer.com/ |
| 15 | Develon | https://na.develon-ce.com/ |
| 16 | JLG Industries | https://www.jlg.com/ |
| 17 | The Toro Company | https://www.toro.com/ |
| 18 | Ditch Witch | https://www.ditchwitch.com/ |
| 19 | Link-Belt Cranes | https://www.linkbelt.com/ |
| 20 | Manitowoc Cranes | https://www.manitowoc.com/ |

### 3.4 Development setup

| Tool | Purpose |
|---|---|
| VS Code (Windows) | Writing the Python, YAML and dashboard files |
| Git 2.52 | Version control; local clone of the GitHub repository |
| Databricks CLI v1.19.0 | Installed with `winget install Databricks.DatabricksCLI`; used to validate, deploy and run the bundle, query the warehouse and sync the Git folder |
| CLI authentication | `databricks auth login --host https://dbc-c0dead6e-f16f.cloud.databricks.com` (browser sign-in, OAuth; no passwords or tokens stored in the project) |
| Databricks Git folder | Workspace copy of the GitHub repository for viewing the code inside Databricks |

### 3.5 Libraries and external services

| Item | Version / detail | Used by |
|---|---|---|
| Serverless environment | Version 2 (Python 3) | All job tasks |
| `beautifulsoup4` | 4.12.3 (installed by the job) | HTML parsing in the scraper |
| `requests` | ≥ 2.31 | HTTP requests to websites and the geocoding service |
| OpenStreetMap Nominatim | Free address-to-coordinates service; max 1 request per second | Map coordinates for headquarters |

---

## 4. Databricks Medallion Architecture Planned

### 4.1 Data flow

```
 LANDING (Volume)                 BRONZE                     SILVER                              GOLD                          DASHBOARD
 ──────────────────               ───────────────────        ─────────────────────────────       ───────────────────────       ──────────────────────────
 US_Equipment_Companies.csv  ──►  equipment_companies  ──►   site_urls        (page inventory)  ──► dim_company           ──►  KPI tiles
 (20 companies, 3 columns)        (CSV as a table)           site_pages       (page content)        company_categories         Headquarters map + directory
                                                             scrape_status    (per company)         electric_products          Product portfolio heatmap
                                                             company_contacts (HQ, phone, map)      tech_mentions              Electric & battery products
                                                                                                    news_by_year               Technology focus heatmap
                                                                                                                               News activity per year
                                       ▲ company websites (sitemaps + pages)   ▲ OpenStreetMap (coordinates)
```

After the gold tables are rebuilt, the job's last task refreshes the dashboard, so it always shows the latest data.

### 4.2 Layers

| Layer | Location | Content | Built by | Write mode |
|---|---|---|---|---|
| **Landing** | Volume `/Volumes/equipmentcompanies/landing/equipmentcompanies/` | The uploaded CSV file, untouched | Manual upload | — |
| **Bronze** | `equipmentcompanies.bronze` | The CSV as a Delta table; values kept as text, exactly as in the file, plus audit columns | `src/bronze/landing_to_bronze.py` | Overwrite |
| **Silver** | `equipmentcompanies.silver` | Website data: page inventory, scraped page content, scrape status, extracted contact details | `src/silver/scrape_sites.py`, `src/silver/company_contacts.py` | Overwrite |
| **Gold** | `equipmentcompanies.gold` | Cleaned, normalized, aggregated tables — one per dashboard need | `src/gold/build_gold.py` | Overwrite |
| **Dashboard** | AI/BI dashboard | Six visuals reading only gold tables | `src/dashboard/equipment_companies.lvdash.json` | Deployed by bundle |

### 4.3 Pipeline orchestration

The Databricks job `equipment_companies_pipeline` runs four Python script tasks on serverless compute and then a dashboard refresh task, in order:

```
bronze_ingest ──► silver_scrape ──► silver_contacts ──► gold_build ──► dashboard_refresh
   (~1 min)         (~15–20 min)        (~1 min)          (~1 min)        (~1 min)
```

| Task | Script | Depends on | Environment | Timeout |
|---|---|---|---|---|
| `bronze_ingest` | `src/bronze/landing_to_bronze.py` | — | `default` | default |
| `silver_scrape` | `src/silver/scrape_sites.py` (+ `site_rules.py`) | `bronze_ingest` | `scraping` (BeautifulSoup, requests) | 60 min |
| `silver_contacts` | `src/silver/company_contacts.py` | `silver_scrape` | `scraping` | 15 min |
| `gold_build` | `src/gold/build_gold.py` (+ `categories.py`) | `silver_contacts` | `default` | 30 min |
| `dashboard_refresh` | Dashboard task: refreshes "Equipment Companies Dashboard" on the Serverless Starter Warehouse | `gold_build` | — | 15 min |

The job has no schedule; it is run on demand from **Jobs & Pipelines** or with the CLI. `databricks bundle run equipment_companies_pipeline --only "silver_contacts+"` rebuilds contacts, gold and the dashboard from the last scrape without visiting the websites again; `--only "gold_build+"` rebuilds only gold and the dashboard. If `gold_build` fails, the refresh is skipped and the dashboard keeps the last good data.

### 4.4 Deployment architecture

```
 Developer PC (VS Code)                     GitHub                           Databricks workspace
 ──────────────────────                     ──────                           ─────────────────────
 Repository files  ── git push ──────────►  Venkat2701/DataBricks  ── pull ─► Git folder (view/edit code)
        │
        └── databricks bundle deploy ─────────────────────────────────────►  .bundle/equipment_companies/dev/files  (scripts)
                                                                             Job: equipment_companies_pipeline
                                                                             Dashboard: Equipment Companies Dashboard
```

- `databricks.yml` defines the bundle and the workspace; `resources/*.yml` define the job and the dashboard.
- `bundle deploy` uploads the scripts unchanged (Python is not compiled), creates or updates the job through the Jobs API, and creates or updates and publishes the dashboard. A state file remembers the job and dashboard IDs, so redeploys update them instead of creating duplicates.
- The job runs the deployed copy of the scripts; the Git folder is a view of GitHub. Every change is pushed and deployed from the same commit.

### 4.5 Design principles

| Principle | How it is applied |
|---|---|
| Idempotent runs | Every table is fully replaced on each run; reruns never duplicate rows |
| Raw bronze | Bronze keeps CSV values as text, unchanged; cleaning happens later |
| Traceability | Bronze stores the source file; silver stores each page's URL, scrape time and the source page of every extracted fact |
| Responsible scraping | robots.txt followed, ≥ 1 second between requests per site, honest bot name in the User-Agent, blocks and captchas respected |
| Failure isolation | One broken website never stops the others; its status is recorded instead |
| Configuration as code | Per-site settings (`site_rules.py`) and category rules (`categories.py`) are plain, reviewable files |
| One source of truth | GitHub holds all code, job and dashboard definitions |

### 4.6 Repository structure

```
databricks.yml                                Bundle configuration (workspace, target)
resources/equipment_pipeline.job.yml          Job definition: 5 tasks, environments
resources/equipment_dashboard.dashboard.yml   Dashboard definition (file, warehouse)
src/bronze/landing_to_bronze.py               Landing CSV -> bronze table
src/silver/scrape_sites.py                    Company websites -> silver tables
src/silver/site_rules.py                      Per-site scraping settings
src/silver/company_contacts.py                Scraped pages -> HQ address, phone, founded year, coordinates
src/gold/build_gold.py                        Silver -> cleaned gold tables
src/gold/categories.py                        Standard equipment categories and matching rules
src/dashboard/equipment_companies.lvdash.json The AI/BI dashboard
README.md                                     Technical reference
docs/                                         Project documentation
```

---

## 5. Steps of Building the Dashboard

The project followed the 10-step workflow plan. Steps 1–6 prepared the source and the platform; steps 7–10 built the pipeline and the dashboard.

| Step | Name | Result |
|---|---|---|
| 1 | Source setup | Local project folder for the Python scripts |
| 2 | Publish the source to GitHub | Repository `Venkat2701/DataBricks` |
| 3 | Connect the GitHub repo in Databricks | Databricks Git folder linked to the repository |
| 4 | Get the company sites | 15 US equipment companies and their websites (20 after the 07/10/2026 extension) |
| 5 | Convert to CSV | `US_Equipment_Companies.csv` |
| 6 | Upload to the landing volume | CSV in `equipmentcompanies.landing` volume |
| 7 | Bronze stage | `bronze.equipment_companies` (15 rows) |
| 8 | Silver stage | 4 silver tables: website inventory, pages, status, contacts |
| 9 | Gold stage | 5 cleaned dashboard tables |
| 10 | Build dashboard | AI/BI dashboard with 6 visuals, deployed and published |

### Step 1 — Source setup

- Created a local project folder (`DataBricks`) to hold all Python scripts and configuration.
- Initialized it as a Git repository on branch `main`.

### Step 2 — Publish the source to GitHub

- Created the GitHub repository `Venkat2701/DataBricks` and set it as the local repository's `origin` remote.
- The first push was rejected (HTTP 403) because the GitHub account signed in on the development PC had no write access to the repository. **Fix:** that account was added as a collaborator on the repository; pushes worked from then on.
- Every stage was committed and pushed to `main`:

| Commit | Content |
|---|---|
| `18b8e5f` | Bronze stage: landing CSV to bronze table, deployed as a Databricks job |
| `d077d15` | Silver stage: scrape company websites into silver tables |
| `89411d5` | Company contacts, gold tables and the AI/BI dashboard |

### Step 3 — Connect the GitHub repo in Databricks

- Created a **Git folder** in the Databricks workspace linked to `github.com/Venkat2701/DataBricks` (branch `main`).
- After each push, the Git folder is updated to the latest commit (Databricks UI **Pull**, or `databricks repos update <id> --branch main` from the CLI), so the code seen in Databricks always matches GitHub.

### Step 4 — Get the company sites

- Collected 15 US-based equipment companies and their official website addresses with AI assistance (list in section 3.3). The list covers construction (Caterpillar, Case, Komatsu, Volvo CE, Hitachi, Develon, JCB), compact equipment (Bobcat, Wacker Neuson), agriculture (John Deere, AGCO, New Holland, Kubota), and specialty equipment (Terex, Vermeer).

### Step 5 — Convert as CSV

- Saved the list as `US_Equipment_Companies.csv` with the columns **Serial No**, **Company Name** and **Company Site URL** (the plan named two columns; the company name was added because the dashboard needs it).

### Step 6 — Upload to volume

- Created the catalog `equipmentcompanies` with the schemas `landing`, `bronze`, `silver` and `gold`, and the volume `landing.equipmentcompanies`.
- Uploaded `US_Equipment_Companies.csv` (747 bytes) to `/Volumes/equipmentcompanies/landing/equipmentcompanies/` through **Catalog → Upload to this volume**.

### Step 7 — Bronze stage

**Goal:** turn the landing CSV into a Delta table in the bronze schema.

#### 7.1 Tooling and deployment setup

Before writing the first pipeline script, deployment was set up so that every stage could be deployed as code:

1. Installed the Databricks CLI (`winget install Databricks.DatabricksCLI`, v1.19.0).
2. Signed in with `databricks auth login` (browser OAuth; profile `DEFAULT`).
3. Created the bundle configuration `databricks.yml` (bundle `equipment_companies`, target `dev`, workspace host).
4. Created `resources/equipment_pipeline.job.yml` defining the job `equipment_companies_pipeline` with serverless compute (environment version 2) and `max_concurrent_runs: 1`.
5. Added `.gitignore` (excludes the CLI's local `.databricks/` state and Python caches) and `README.md`.

**Pipeline style decision:** a Databricks **Job with Python script tasks** was chosen over a Lakeflow Declarative Pipeline (DLT), because the silver stage visits websites — ordinary Python that runs naturally as a job task — and one style for all stages keeps the pipeline simple.

#### 7.2 Bronze script — `src/bronze/landing_to_bronze.py`

| Processing step | Detail |
|---|---|
| Read | Every `*.csv` file in the landing volume, header row used, all values read as text |
| Header clean-up | Removes a possible byte-order mark (added by Excel) and spaces from header names |
| Validation | Stops with an error if an expected column is missing or if no rows were read (the table is then left unchanged) |
| Rename | `Serial No` → `serial_no`, `Company Name` → `company_name`, `Company Site URL` → `company_site_url` (Delta tables do not allow spaces in column names) |
| Audit columns | `_source_file` (file the row came from, from file metadata) and `_ingested_at` (load time) |
| Write | `equipmentcompanies.bronze.equipment_companies`, mode overwrite |

**Bronze table — `bronze.equipment_companies`**

| Column | Description |
|---|---|
| `serial_no` | Serial No from the CSV (text) |
| `company_name` | Company Name from the CSV |
| `company_site_url` | Company Site URL from the CSV |
| `_source_file` | Path of the CSV file the row came from |
| `_ingested_at` | Time the row was loaded |

#### 7.3 Deploy, run and verify

- `databricks bundle validate` → `databricks bundle deploy` (created the job) → `databricks bundle run equipment_companies_pipeline`.
- Run result: *"Wrote 15 rows to equipmentcompanies.bronze.equipment_companies"*.
- Verified with a SQL query on the Serverless Starter Warehouse: 15 rows, all names and URLs matching the CSV, clean column names.

### Step 8 — Silver stage

**Goal:** visit every company website (15 in the first release, 20 now), extract their data and store it as silver tables.

#### 8.1 Feasibility check: can Databricks reach the websites?

Databricks Free Edition limits outbound internet access, so before designing the scraper a one-time test run on serverless compute requested each company's homepage.

| Result | Companies |
|---|---|
| Reachable (HTTP 200) | 14 of 15 |
| Blocked (HTTP 403) | Caterpillar |
| Suspiciously small pages | Bobcat, Volvo CE (later explained: captcha page and country chooser) |

#### 8.2 Website probe

A second one-time run inspected each site's `robots.txt` (rules for bots), its sitemap (the list of page addresses published for search engines), its size and whether its pages contain real text or are built by JavaScript. Main findings:

| Finding | Companies | Effect on design |
|---|---|---|
| Large sitemaps; ~15,000 US pages in total (excluding spare parts) | All | Scraping everything would take hours → limit content scraping |
| 179,854 spare-part pages | Komatsu | Exclude parts pages |
| Homepage built by JavaScript (66 characters of text) | John Deere | Expect little text |
| Country-chooser homepage | Volvo CE, Wacker Neuson | Start from the US section (`/united-states/en-us/`, `/us/`) |
| No sitemap | Terex | Discover pages by following links |
| Mixed regions and languages in sitemaps | Case, Bobcat, New Holland, Hitachi, Vermeer… | Keep only the US English section |
| Blocked | Caterpillar | Record as blocked; no workaround |

**Scope decision:** keep the **inventory of every US page** (from sitemaps — fast, and complete for counting products and news) and scrape the **content of up to 100 key pages per company**.

#### 8.3 Scraper design — `src/silver/scrape_sites.py` and `site_rules.py`

**Per-site settings (`site_rules.py`)**, keyed by the company's website host:

| Setting | Meaning | Example |
|---|---|---|
| `base_url` | Only pages under this address are listed and scraped (the US English section) | `https://www.volvoce.com/united-states/en-us/` |
| `start_url` | First page scraped | AGCO: `/us/en/home.html` |
| `sitemaps` | Sitemap files to read (default: from robots.txt) | Wacker Neuson: `/us/sitemap.xml` |
| `exclude` | Pages to ignore | Komatsu spare parts; Terex other-language sections |
| `product_pattern` | Which addresses are product pages | Deere: `products-and-solutions/…` |
| `category_segment` | Which part of the address names the product category | Komatsu: `products/equipment/<category>` |

Companies without settings are scraped from their bronze URL with defaults, so new companies can be added to the CSV without code changes.

**How one website is processed** (all sites run in parallel threads):

1. **robots.txt** — read the site's rules and any crawl delay; if the start page is not allowed, record the site as blocked.
2. **Sitemaps** — read the sitemap files (following sitemap indexes and compressed `.gz` files, up to 150 files), keep only addresses inside `base_url`, drop query strings and duplicates.
3. **Classify each address** into a page type (`home`, `product`, `about`, `parts_service`, `news`, `dealer`, `careers`, `document`, `other`) and record its section and category from the address.
4. **Start page** — scrape it; if it is blocked or shows a captcha, stop and record that status.
5. **Choose pages** — up to 100 per company in this order: start page → up to 10 contact/locations/about pages → up to 80 product pages (category pages first) → about → parts & service → other → news → dealer → careers.
6. **Scrape each page** — title, meta description, main heading, sub-headings, main text (menus, headers, footers removed), word count, structured data (JSON-LD), internal link count, and every phone number and US address found anywhere on the page (header and footer included).
7. **No sitemap** (Terex) — discover pages by following links from the start page.
8. **Record status** — `ok`, `partial`, `js_rendered`, `captcha`, `blocked` or `error`, with an explanation.

**Safeguards**

| Safeguard | Setting |
|---|---|
| Delay between requests to the same site | ≥ 1 second (longer if robots.txt asks) |
| Request timeout | 20 seconds |
| Time limit per site | 20 minutes |
| Stop a site after repeated hard failures | 5 blocked/timeout/server-error pages in a row (missing pages, HTTP 404, do not count) |
| Skip page types that arrive empty | After 10 pages of one type with almost no text (JavaScript-built), that type is skipped for the site |
| Text kept per page | 50,000 characters |
| User-Agent | Browser-compatible, identifying the bot: `EquipmentDashboardBot/1.0 (+github.com/Venkat2701/DataBricks)` |

#### 8.4 Silver tables

| Table | One row per | Main columns | Rows (latest run) |
|---|---|---|---|
| `silver.site_urls` | US page address found | `url`, `relative_path`, `page_type`, `section`, `category`, `depth`, `lastmod`, `source` | 14,578 |
| `silver.site_pages` | page scraped | `title`, `meta_description`, `h1`, `headings`, `body_text`, `word_count`, `json_ld`, `contact_phones`, `contact_addresses`, `fetch_status`, `http_status` | 1,767 (1,732 OK) |
| `silver.scrape_status` | company | `status`, `message`, `urls_found`, `pages_ok`, `pages_failed`, `median_word_count`, `duration_seconds` | 20 |
| `silver.company_contacts` | company | `hq_address`, `city`, `state`, `zip`, `latitude`, `longitude`, `phone`, `founded_year`, `geocoded_city`, `*_match`, `*_source_url` | 20 |

Row counts are from the latest run with 20 companies; the validation in 8.6 was done on the first release (15 companies).

#### 8.5 Issues found and fixed during the silver build

| Issue | Cause | Fix |
|---|---|---|
| Silver task failed: `__file__` not defined | Databricks runs script tasks through `exec()`, so a script cannot find its own folder that way | Read the script path from the code object (`inspect`) to import `site_rules.py` |
| Terex stopped after 5 failures | Link-following wandered into other-language copies (`/de/`, `/fr/`…) that return 404, and 404s counted as failures | Exclude other-language sections for Terex; only blocks, timeouts and server errors count as failures |
| JavaScript sites used their whole page allowance on empty pages | Deere and Kubota build pages in the browser | Skip a page type after 10 empty pages in a row |
| Caterpillar status unclear | The site timed out instead of returning 403 | Status message now states the start-page error |
| Long product pages cut | Text limited to 10,000 characters (102 pages affected) | Limit raised to 50,000 characters |
| HQ address and phone often missing | Contact details sit in page footers, which were stripped, and contact pages were crowded out by product pages | Collect phones/addresses from the whole page before stripping; scrape up to 10 contact/about pages first |

#### 8.6 Silver data validation

All checks were run as SQL on the warehouse.

| Check | Result |
|---|---|
| Duplicate rows / missing keys | None in any silver table |
| Silver companies match bronze (serial and name) | All 15 match |
| Page addresses outside each company's US section | 0 |
| Pages scraped successfully | 1,353 of 1,366 |
| Non-English text | Negligible (1 page) |

Data-quality findings and how they were handled:

| Finding | Handling |
|---|---|
| Same text on several pages (Kubota "Loading…" pages; Komatsu category pages reached by several addresses) | Removed in gold (one page per identical text) |
| 3 pages redirected to other websites (e.g. Komatsu → komatsupress.com) | Excluded in gold |
| No meta description on most Hitachi and Komatsu pages | Dashboard falls back to the main heading or first sentence |
| No page dates in Terex's and Vermeer's sitemaps | Those companies are left out of date-based charts |
| Each site names its categories differently | Standard category mapping in gold (step 9) |
| Facts mined from free text (founded year, employees) unreliable | Stricter rules for founded year (8.8); employee counts not used |

#### 8.7 Dashboard visual planning and data gap analysis

Before building gold, 17 candidate visuals were listed and each was checked against the silver data. Six were selected because they answer the key business questions and have data for most companies.

| Selected visual | Reason |
|---|---|
| KPI tiles | Instant overview, data for all companies |
| Headquarters map + directory | Location view requested in the plan; needs HQ data (8.8) |
| Product portfolio heatmap | Strongest comparison; sitemap data exists for 14 companies |
| Electric & battery products | Topical; data for 14 companies |
| Technology focus heatmap | Shows each company's emphasis; data for 12–13 companies |
| News activity per year | Shows publishing activity; data for 9 companies |

| Dropped visual | Reason |
|---|---|
| Spec comparison (horsepower vs weight) | Only ~6 brands publish comparable specs; machines differ in size |
| Product catalog table | Too detailed for an overview dashboard |
| Dealer network | Dealer page counts reflect site structure, not network size |
| Social media presence | Links to other sites not stored; only 3 companies |
| Website freshness / footprint | Describe the websites, not the companies |
| Promotions | Snapshot that dates within weeks |

**HQ data decision:** headquarters address and phone are taken **only from the company websites** (no manually typed reference file and no third-party source), accepting that some companies will have no map pin.

#### 8.8 Company contact details — `src/silver/company_contacts.py` (task `silver_contacts`)

This task creates `silver.company_contacts` from the pages already scraped; it does not visit the company websites again.

| Value | Rule (best first) |
|---|---|
| **Address** | 1) preceded by words like "headquarters" / "corporate office"; 2) in structured data; 3) on a contact, locations or about page; then the one found on the most pages |
| **City** | The website's own city name, with street words removed ("River Green Parkway Duluth" → "Duluth") |
| **Phone** | The number printed right after the chosen address; otherwise the same order as for addresses (phone links count like structured data); fax numbers skipped |
| **Founded year** | Earliest "since / founded / established <year>" that the website states about itself — in the homepage title/description, or in a home/about sentence using "we"/"our" that is not about a facility, award or partner |
| **Coordinates** | OpenStreetMap Nominatim lookup by ZIP code (1 request per second; only new or changed addresses) |

Each value is stored with how it was chosen (`*_match`), the page it came from (`*_source_url`) and the surrounding text, so it can be checked.

**Iterations:** the first version picked a Bobcat factory address (the word "headquarters" appeared *after* it), Komatsu's customer-support phone instead of its HQ line, cities with street words, and founded years from unrelated sentences (a city founded in 1879, an award since 1969, a facility established in 2022). The rules above fixed each case.

**Result (first release):** HQ address and coordinates for **11 of 15** companies, phone for **11**, founded year for **4** (each verified against the website text). No address was published in a readable form by John Deere (JavaScript pages), Caterpillar (no response), New Holland and Hitachi.

**Result (20 companies):** HQ address and coordinates for **15**, phone for **15**, founded year for **5** (Manitowoc added 1902); Link-Belt has no data (see "Enhancements after the first release").

### Step 9 — Gold stage

**Goal:** clean and normalize the silver data and build one table per dashboard need. Script: `src/gold/build_gold.py`, with category rules in `src/gold/categories.py`.

#### 9.1 Cleaning and normalization

| Technique | Applied to |
|---|---|
| Remove redundant data | Pages with identical text (kept once per company); pages redirected to other websites |
| Remove non-product pages | Compare tools, finance, parts, offers, warranty/protection plans, legal, "build and price" pages listed under product sections |
| Whitespace normalization | All text: trimmed, repeated spaces collapsed |
| Case normalization | Cities in Title Case, state codes in upper case, product names in Title Case, website URLs in lower case |
| Name normalization | Short **brand** for charts by removing legal/regional suffixes ("Kubota Tractor Corporation" → "Kubota", "JCB North America" → "JCB") |
| Format normalization | Phones as `000-000-0000`; internal codes removed from product names ("…Greens Mower Ntu1m0e" → "…Greens Mower") |
| Category standardization | 27 standard equipment categories (9.2) |

#### 9.2 Standard equipment categories

Each product page address is matched against ordered rules in `categories.py`; the first match gives its standard category and segment.

| Segment | Standard categories |
|---|---|
| Construction | Mini Excavators, Excavators, Skid Steer & Track Loaders, Backhoe Loaders, Wheel Loaders, Dozers, Motor Graders, Haulers & Dump Trucks, Telehandlers & Material Handling, Compaction, Aerial Work Platforms, Cranes, Concrete & Paving, Drills & Trenchers, Light & Power Equipment |
| Agriculture | Tractors, Combines & Harvesting, Hay & Forage, Seeding & Tillage, Sprayers & Crop Care, Precision Technology |
| Turf & Utility | Mowers & Turf, Utility Vehicles |
| Forestry | Forestry & Tree Care |
| Mining | Mining Equipment |
| Other | Engines & Powertrain, Attachments |

The rules were refined by reviewing the results; examples:

| Problem found | Fix |
|---|---|
| New Holland tractors counted as telehandlers (addresses `tractors-telehandlers/…`) | Tractors checked before telehandlers |
| JCB "telescopic handlers", Komatsu "room-and-pillar", Terex brand pages not matched | Matching words added |
| Vermeer feed mixers counted as concrete equipment | Mapped to Hay & Forage (livestock) |
| Deere generator-drive engines counted as power equipment | Engines checked first |
| Tillage rollers counted as compaction | Compaction rule excludes tillage |

Product pages without a category fell from 512 (12%) to **290 of 4,166 (7%)** in the first release, and stand at **354 of 5,369 (6.6%)** with 20 companies — mostly Komatsu's industrial machine-tool pages, which are outside this dashboard's scope. The job log lists the most common unmatched pages, so new rules can be added.

#### 9.3 Technology themes

| Theme | Words searched in page text |
|---|---|
| Electric & Battery | electric, battery, electrified, zero emission |
| Autonomy | autonomous, autonomy, driverless, self-driving |
| Telematics & Connectivity | telematics, connectivity, remote monitoring, fleet management |
| GPS & Precision | GPS, precision ag, grade control, machine control |
| Alternative Fuels | hydrogen, HVO, renewable diesel, biodiesel |
| Emissions | Tier 4, Stage V, emissions |
| Sustainability | sustainability, carbon, net zero |

A company's share for a theme = pages mentioning it ÷ pages with text (≥ 20 words). Companies with fewer than 20 text pages are marked as having no data.

#### 9.4 Gold tables

| Table | One row per | Main columns | Rows | Feeds |
|---|---|---|---|---|
| `gold.dim_company` | company | `company_name`, `brand`, `website`, `tagline`, `hq_address`, `city`, `state`, `zip`, `latitude`, `longitude`, `phone`, `founded_year`, `data_status`, `data_note`, `product_pages`, `has_location` | 20 | KPI tiles, map, directory |
| `gold.company_categories` | company × category | `brand`, `segment`, `category`, `product_pages`, `example_url` | 153 | KPI tiles, portfolio heatmap |
| `gold.electric_products` | electric/battery product | `brand`, `product_name`, `category`, `segment`, `url`, `name_source` | 138 | KPI tiles, electric chart |
| `gold.tech_mentions` | company × theme | `brand`, `theme`, `text_pages`, `pages_mentioning`, `pct_pages`, `has_data` | 140 | Technology heatmap |
| `gold.news_by_year` | company × year (last 5 years) | `brand`, `year`, `news_pages` | 45 | KPI tiles, news chart |

### Step 10 — Build dashboard

**Goal:** an insightful Equipment Company dashboard in Databricks using the gold tables.

#### 10.1 Approach

The dashboard is a **Databricks AI/BI dashboard** defined as code in `src/dashboard/equipment_companies.lvdash.json` and deployed by the same bundle as the job (`resources/equipment_dashboard.dashboard.yml`, Serverless Starter Warehouse). The widget formats follow Databricks' official AI/BI dashboard reference (databricks/databricks-agent-skills on GitHub).

#### 10.2 Datasets

| Dataset | Source | Purpose |
|---|---|---|
| Companies | `gold.dim_company` | Company count, directory table |
| HQ locations | `gold.dim_company` (rows with coordinates) | Map, "HQ found" tile |
| Product categories | `gold.company_categories` (attachments excluded), with each category's share of the company's product pages | Portfolio heatmap, category and product tiles |
| Electric products | `gold.electric_products` | Electric chart and tile |
| Technology mentions | `gold.tech_mentions` (companies with data) | Technology heatmap |
| News per year | `gold.news_by_year` | News chart, "news this year" tile |

All six dataset queries were tested on the warehouse before deployment.

#### 10.3 Layout (one page, 12-column grid)

| Row | Widgets |
|---|---|
| 1 | Title and subtitle |
| 2 | Filters: **Company** (applies to all widgets) and **Segment** (portfolio and electric charts) |
| 3 | Six KPI tiles: Companies · HQ found on website · Equipment categories · Machine product pages · Electric products · News pages this year |
| 4 | Headquarters map (pins coloured by company) · Company directory table (company, clickable website opening in a new tab, HQ, phone, founded, website data status) |
| 5 | Product portfolio heatmap (company × standard category, grouped construction → agriculture → turf → forestry → mining) |
| 6 | Electric & battery products (bar) · Technology focus heatmap (company × theme) |
| 7 | News activity per year (line) · About-this-data notes |

A consistent, colour-blind-safe palette (Okabe-Ito) is set in the dashboard theme.

#### 10.4 Deploy and publish

- `databricks bundle deploy` created the dashboard **"Equipment Companies Dashboard"** (state: active, published; 6 datasets, 17 widgets).
- The warehouse query history confirmed that every widget's query ran successfully and returned data.

#### 10.5 Final full run

A final full job run (`bronze_ingest → silver_scrape → silver_contacts → gold_build`) completed successfully and refreshed every table used by the dashboard.

#### 10.6 Issue: dashboard widgets blocked on the office network

| | |
|---|---|
| **Symptom** | Every widget shows "Unable to render visualization"; filters and text load |
| **Diagnosis** | Queries succeed on the warehouse, but the office network's **Sophos Web Protection** blocks the dashboard's result download (`/ajax-api/2.0/lakeview-query/query-results/…?format=arrows`). Dashboard results use the binary Apache Arrow format, which Sophos classifies as "Other Executables" and replaces with an "Access denied" page |
| **Impact** | On the office network the dashboard cannot display data; the data, queries and dashboard definition are unaffected |
| **Fix** | IT exception in the Sophos file-type rule for `https://dbc-c0dead6e-f16f.cloud.databricks.com/ajax-api/2.0/lakeview-query/query-results/*` (or `*.cloud.databricks.com`) |
| **Status** | Resolved: the dashboard displays its data (confirmed 07/10/2026). If the symptom returns on a network with the same filter, the fix above applies |

### Enhancements after the first release (07/10/2026)

#### E.1 Clickable Website column in the company directory

- Added the company's website to the **Company directory** table and widened the table (map 5 columns, table 7 of the 12-column grid).
- The documented link setting of the newer table format (version 2) was saved but ignored by the dashboard, so the table was switched to the **version 1** table format used in Databricks' own example dashboards. The Website column uses `displayAs: link` with `linkOpenInNewTab: true`, so clicking a website opens the company's site in a new browser tab.

#### E.2 Automatic dashboard refresh (task 5)

- Running the job updated the gold tables, but the dashboard kept showing its previous (cached) results until someone clicked refresh.
- Added a fifth job task, `dashboard_refresh`, of the Databricks **Dashboard** task type. It runs after `gold_build` and refreshes the Equipment Companies Dashboard on the Serverless Starter Warehouse. The dashboard is referenced through the bundle (`${resources.dashboards.equipment_companies_dashboard.id}`), so the task keeps working if the dashboard is recreated.
- Tested on its own and as part of a gold rebuild: the task succeeded and the warehouse history showed the dashboard's queries re-running.

#### E.3 Five more companies

Five companies were added to the CSV (serial numbers 16–20) and the job was run without code changes. Everything ran end to end and the new companies appeared in every visual; the first run then showed what needed tuning:

| Finding | Cause | Fix |
|---|---|---|
| Volvo CE dropped from 1,178 page addresses to 1 and lost its HQ | Volvo moved its US section from `/united-states/en-us/` to `/en-us/` (a website change, unrelated to the new companies) | Updated Volvo's `base_url` in `site_rules.py` |
| JLG and Manitowoc took their whole sites in every language (4,341 and 14,860 addresses); Manitowoc hit the 20-minute site limit | New companies have no site settings, so the default is the whole site | Site settings added: JLG `/en/` (skipping its `directaccess` parts portal); Manitowoc English pages only, skipping 8 other-language sections and dealer pages, with product pages under its crane brands (Grove, Potain, National Crane, Manitowoc, Krupp, Shuttlelift) |
| Toro: 0 addresses read from its sitemap | Toro's sitemap is a plain-text list, not XML | The scraper now also reads plain-text sitemaps; Toro settings start at `/en/` |
| Ditch Witch: most sitemap entries were uploaded images and files | WordPress uploads listed in the sitemap | Site settings skip `wp-content`; product sections set (trenchers, vacuum excavation, directional drills, stand-on skid steers, trenchless tools) |
| Crane products had no category | No crane category existed | New **Cranes** category (incl. Krupp all-terrain and truck-mounted lines); trenchless/HDD tooling added to Drills & Trenchers; JLG vertical lifts, low-level access and stock pickers added to Aerial Work Platforms; "used equipment" pages excluded |
| JLG engine-powered lifts counted as engines | The Engines rule matched "engine" in "engine-powered" | Engines rule now ignores "engine-powered" |
| Brand names "The Toro", "JLG Industries", "Manitowoc Cranes" | Name suffix/prefix rules did not cover them | Brand rules now drop a leading "The" and the suffixes "Industries" and "Cranes" |
| Toro's city read "South Bloomington" | "…Lyndale Avenue **South**, Bloomington, MN": the street's direction word stuck to the city | When the scraped city is the looked-up city plus an extra leading word, the looked-up city is used; the looked-up city is now stored in `geocoded_city` |
| Dashboard subtitle said "15 US equipment manufacturers" | Fixed text | Subtitle no longer states a number; the Companies tile shows the count |
| Link-Belt: no data | The website returns HTTP 403 to automated visits | None (respected, like Caterpillar) |

Result after the fixes:

| | First release (15) | Now (20) |
|---|---|---|
| Companies | 15 | 20 |
| HQ on the map | 11 | 15 |
| Phones | 11 | 15 |
| Equipment categories in use | 25 | 26 |
| Electric products | 113 | 138 |
| Product pages without a category | 7% | 6.6% |
| Full run time | ~15 min | ~20 min (Manitowoc asks for 10 seconds between requests) |

### Operating the solution

| Task | Command / action |
|---|---|
| Sign in (once per PC) | `databricks auth login --host https://dbc-c0dead6e-f16f.cloud.databricks.com` |
| Deploy code, job and dashboard | `databricks bundle deploy` (run from the repository folder) |
| Run the full pipeline (~20 min) | `databricks bundle run equipment_companies_pipeline`, or **Run now** in Jobs & Pipelines; the dashboard refreshes at the end |
| Rebuild contacts, gold and dashboard only | `databricks bundle run equipment_companies_pipeline --only "silver_contacts+"` |
| Rebuild gold and dashboard only | `databricks bundle run equipment_companies_pipeline --only "gold_build+"` |
| Add a company | Add a row to the CSV (same headers, next serial number), upload it to the landing volume replacing the old file, run the job, then add `site_rules.py` settings based on its first results |
| A company's data suddenly drops | Check its `urls_found` in `silver.scrape_status`; websites change their structure (as Volvo CE did) and its `base_url` may need updating |
| Improve category mapping | Edit `src/gold/categories.py`, deploy, rebuild gold |
| Edit the dashboard | Edit the `.lvdash.json` file and deploy (edits made in the Databricks UI are overwritten by the next deploy unless copied back) |

---

## 6. Dashboard Output

**Dashboard:** *Equipment Companies Dashboard* — Databricks → **Dashboards**
Published link: `https://dbc-c0dead6e-f16f.cloud.databricks.com/dashboardsv3/01f1c18f09c2163bb5e31c7ebae6e94f/published`

> **Note:** The values below come from the gold tables behind each widget (latest run with 20 companies, 07/10/2026). The dashboard refreshes automatically at the end of every job run. Dashboard screenshots can be inserted here.

### 6.1 KPI tiles

| Tile | Value |
|---|---|
| Companies | **20** |
| HQ found on website | **15** |
| Equipment categories | **26** |
| Machine product pages | **4,058** (attachments excluded) |
| Electric products | **138** |
| News pages this year (2026) | **278** |

### 6.2 Headquarters map and company directory

The map shows a pin for each of the 15 companies whose headquarters address was found; the directory lists all 20, and each website is a link that opens in a new tab.

| Company | Website | HQ (city, state, ZIP) | Phone | Founded | Website data |
|---|---|---|---|---|---|
| AGCO | agcocorp.com | Duluth, GA 30096 | 770-813-9200 | 1990 | Full data |
| Bobcat | bobcat.com | West Fargo, ND 58078 | 701-241-8700 | – | Full data |
| Case | casece.com | Racine, WI 53402 | 866-542-2736 | – | Full data |
| Caterpillar | cat.com | Not published on website | – | – | No data: website did not respond |
| Develon | na.develon-ce.com | Suwanee, GA 30024 | 678-714-6000 | – | Full data |
| Ditch Witch | ditchwitch.com | Perry, OK 73077 | 800-266-3255 | – | Full data |
| Hitachi | hitachicm.us | Not published on website | – | – | Full data |
| JCB | jcb.com | Pooler, GA 31322 | – | – | Full data |
| JLG | jlg.com | McConnellsburg, PA 17233 | 717-485-5161 | – | Full data |
| John Deere | deere.com | Not published on website | – | 1837 | Limited: pages built by JavaScript |
| Komatsu | komatsu.com | Chicago, IL 60631 | 847-437-3888 | – | Full data |
| Kubota | kubotausa.com | Grapevine, TX 76051 | 888-458-2682 | – | Full data |
| Link-Belt | linkbelt.com | Not published on website | – | – | No data: website blocks automated access |
| Manitowoc | manitowoc.com | Milwaukee, WI 53224 | 414-760-4600 | 1902 | Full data |
| New Holland | agriculture.newholland.com | Not published on website | 866-639-4563 | – | Full data |
| Terex | terex.com | Fort Wayne, IN 46818 | 800-678-5961 | – | Full data |
| Toro | toro.com | Bloomington, MN 55420 | 877-345-8676 | – | Full data |
| Vermeer | vermeer.com | Pella, IA 50219 | 641-628-3141 | 1948 | Full data |
| Volvo | volvoce.com | Shippensburg, PA 17257 | 855-235-6014 | – | Full data |
| Wacker Neuson | wackerneuson.com | Menomonee Falls, WI 53051 | 262-255-0500 | 1848 | Full data |

Addresses are as published on each website; for some companies this is a US or North American head office or a business-unit address rather than the global headquarters (for example, Terex's address is its Advance mixer business in Fort Wayne).

### 6.3 Product portfolio heatmap

Each cell is the share of a company's machine product pages in a standard category. Summary:

| Company | Categories | Main categories (product pages) |
|---|---|---|
| John Deere | 20 | Seeding & Tillage 262 · Tractors 219 · Mowers & Turf 162 |
| Kubota | 15 | Hay & Forage 14 · Mowers & Turf 14 · Utility Vehicles 9 |
| Bobcat | 12 | Telehandlers & Material Handling 49 · Light & Power Equipment 41 · Skid Steer & Track Loaders 38 |
| JCB | 11 | Telehandlers & Material Handling 35 · Skid Steer & Track Loaders 15 · Wheel Loaders 15 |
| Case | 9 | Excavators 38 · Skid Steer & Track Loaders 28 · Compaction 22 |
| Develon | 9 | Excavators 21 · Wheel Loaders 13 · Mini Excavators 11 |
| Komatsu | 9 | Mining Equipment 141 · Forestry & Tree Care 49 · Telehandlers & Material Handling 47 |
| Wacker Neuson | 9 | Compaction 36 · Light & Power Equipment 34 · Concrete & Paving 15 |
| AGCO | 7 | Tractors · Precision Technology · Engines & Powertrain (corporate site, few product pages) |
| New Holland | 7 | Hay & Forage 27 · Tractors 26 · Combines & Harvesting 23 |
| Toro | 7 | Mowers & Turf 13 · Tractors 5 · Drills & Trenchers 2 |
| Volvo | 6 | Excavators 97 · Wheel Loaders 26 · Haulers & Dump Trucks 13 |
| Vermeer | 5 | Drills & Trenchers 56 · Hay & Forage 56 · Forestry & Tree Care 29 |
| Ditch Witch | 4 | Drills & Trenchers 87 · Skid Steer & Track Loaders 12 |
| Hitachi | 4 | Excavators 31 · Wheel Loaders 19 · Mining Equipment 18 |
| Terex | 4 | Concrete & Paving 41 · Telehandlers & Material Handling 21 |
| JLG | 3 | Aerial Work Platforms 136 · Telehandlers & Material Handling 27 · Haulers & Dump Trucks 16 |
| Manitowoc | 1 | Cranes 763 |

Segment coverage: construction equipment is offered by **18** of the 20 companies, agriculture by **9**, forestry by **5**, turf & utility by **5** and mining by **4** (mainly Komatsu and Hitachi). Cranes come only from Manitowoc, because Link-Belt's website blocks automated access.

### 6.4 Electric & battery products

| Company | Electric/battery product pages |
|---|---|
| Komatsu | 50 |
| Bobcat | 24 |
| Volvo | 14 |
| Toro | 14 |
| John Deere | 12 |
| JLG | 12 |
| JCB | 6 |
| Case | 3 |
| Wacker Neuson | 3 |

A product counts as electric when its page address or main heading mentions electric or battery. Komatsu's 50 include 10 pages about machinery for producing batteries (its industrial machine-tool business) rather than electric machines.

### 6.5 Technology focus heatmap

Share of each company's text pages mentioning each theme (%):

| Company | Electric | Autonomy | Telematics | GPS & Precision | Alt. fuels | Emissions | Sustainability |
|---|---|---|---|---|---|---|---|
| AGCO | 6.5 | 9.8 | 9.8 | 21.7 | 1.1 | 10.9 | 25.0 |
| Bobcat | 19.4 | 3.2 | 3.2 | 5.4 | 0.0 | 5.4 | 77.4 |
| Case | 42.2 | 0.0 | 58.9 | 13.3 | 0.0 | 42.2 | 1.1 |
| Develon | 5.0 | 2.0 | 65.0 | 14.0 | 0.0 | 30.0 | 2.0 |
| Ditch Witch | 4.5 | 0.0 | 5.7 | 1.1 | 0.0 | 4.5 | 0.0 |
| Hitachi | 23.7 | 0.0 | 19.4 | 0.0 | 0.0 | 16.1 | 6.5 |
| JCB | 12.9 | 0.0 | 2.2 | 0.0 | 8.6 | 14.0 | 20.4 |
| JLG | 16.5 | 0.0 | 11.0 | 0.0 | 0.0 | 7.7 | 3.3 |
| Komatsu | 37.0 | 13.7 | 13.7 | 11.0 | 5.5 | 9.6 | 17.8 |
| Kubota | 9.9 | 1.4 | 8.5 | 1.4 | 0.0 | 0.0 | 4.2 |
| Manitowoc | 4.2 | 0.0 | 1.0 | 0.0 | 1.0 | 21.9 | 32.3 |
| New Holland | 7.2 | 0.0 | 17.5 | 4.1 | 0.0 | 7.2 | 5.2 |
| Terex | 9.6 | 0.0 | 0.0 | 0.0 | 0.0 | 1.2 | 2.4 |
| Toro | 27.0 | 2.2 | 9.0 | 1.1 | 0.0 | 3.4 | 2.2 |
| Vermeer | 26.6 | 4.3 | 4.3 | 4.3 | 0.0 | 25.5 | 3.2 |
| Volvo | 52.7 | 6.6 | 16.5 | 2.2 | 0.0 | 69.2 | 22.0 |
| Wacker Neuson | 51.5 | 0.0 | 3.0 | 0.0 | 0.0 | 28.3 | 2.0 |

John Deere, Caterpillar and Link-Belt are not shown (no page text available). Very high shares such as Bobcat's 77% for sustainability most likely come from a block repeated on every page (for example a sustainability banner) rather than from each page's topic. Volvo's sustainability share fell from 99% to 22% after its website was restructured, which shows how much such repeated blocks affect this measure.

### 6.6 News activity per year

| Company | 2022 | 2023 | 2024 | 2025 | 2026 | Total |
|---|---|---|---|---|---|---|
| John Deere | 0 | 0 | 144 | 144 | 107 | 395 |
| Komatsu | 55 | 70 | 46 | 35 | 49 | 255 |
| Volvo | 21 | 44 | 25 | 135 | 18 | 243 |
| JCB | 12 | 13 | 9 | 29 | 71 | 134 |
| Hitachi | 30 | 13 | 22 | 19 | 19 | 103 |
| Case | 17 | 27 | 12 | 12 | 11 | 79 |
| New Holland | 0 | 21 | 22 | 14 | 3 | 60 |
| Bobcat | 9 | 1 | 1 | 2 | 0 | 13 |
| Develon | 0 | 0 | 0 | 1 | 0 | 1 |

Companies without dated news addresses (AGCO, Kubota, Terex, Vermeer, Wacker Neuson, JLG, Toro, Ditch Witch, Manitowoc, Caterpillar, Link-Belt) are not shown. Volvo's figures changed when its website was restructured in October 2026 (for example 135 news pages dated 2025).

### 6.7 Key insights

- **John Deere has the broadest portfolio** (20 of 26 categories), spanning agriculture, turf, construction and forestry; Kubota (15) and Bobcat (12) follow.
- **Full-line makers versus specialists:** alongside broad portfolios, the extended list adds clear specialists — Manitowoc (cranes only), JLG (mainly aerial work platforms, 136 of 179 machine pages) and Ditch Witch (mainly drills and trenchers, 87 of 101).
- **Construction is the shared battleground:** 18 of the 20 companies offer construction equipment, while only 4 cover mining (mainly Komatsu and Hitachi).
- **Komatsu leads the electric lineup** (50 pages: 27 electric mining machines, 13 electric forklifts, and 10 battery-production machines from its industrial business), followed by Bobcat (24, mostly electric forklifts and warehouse vehicles plus the all-electric T7X loader), Volvo and Toro (14 each) and JLG (12).
- **Electric is a strong message for Volvo (53% of pages), Wacker Neuson (52%) and Case (42%)**; telematics stands out for Develon (65%) and Case (59%); AGCO leads on GPS and precision farming (22%); Volvo also stresses emissions (69%).
- **JCB's news output is rising sharply** (71 news pages in 2026 vs 29 in 2025), while John Deere publishes the most news overall (~144 per year).

### 6.8 Data limitations

- Counts reflect **pages published on each website**, not sales, market share or the number of models in production; sites structure their pages differently.
- **Caterpillar** (website did not respond) and **Link-Belt** (website blocks automated access) have no data; **John Deere** has limited text because its pages are built by JavaScript, and **Kubota's** product pages are built the same way (its other pages are read).
- Headquarters addresses and phones are **as published** on each website and may be regional or business-unit offices; 5 companies publish no readable address (John Deere, Caterpillar, New Holland, Hitachi, Link-Belt).
- Founded year is shown only where the website states it about itself (5 companies).
- Websites change: a restructured site (as Volvo CE's in October 2026) can change a company's figures or need a site-settings update; check `silver.scrape_status` after each run.
- Each run replaces the data; the dashboard shows the latest snapshot, not trends in the companies' websites over time.

### 6.9 Next steps

1. Add dashboard screenshots to this document.
2. Move the hard-coded catalog, volume and warehouse names into Databricks Asset Bundle variables (planned, deferred).
3. Optionally schedule the job (for example weekly); the dashboard already refreshes at the end of every run.
4. Optionally fill the 5 missing headquarters from an open data source (such as Wikidata), labelled as a separate source.
5. Optionally add more category rules for the remaining 6.6% of unmatched product pages.

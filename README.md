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

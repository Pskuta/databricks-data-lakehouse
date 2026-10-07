# databricks-data-lakehouse

Medallion-architecture (Bronze → Silver → Gold) Data Lakehouse on Databricks. It ingests CRM and ERP CSV extracts and produces a star schema for analytics.

![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-FF3621?logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Spark%20SQL-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Delta-00ADD4)

## Overview

**Problem.** Customer, product and sales data sit in two separate source systems (CRM and ERP). Each uses its own key formats, codes and data quality conventions, so the data cannot be analysed together as it is.

**Approach.** The project loads the raw files unchanged, cleans and standardises them, then joins them into a dimensional model.

Key design decisions:
- Three layers (`bronze`, `silver`, `gold`) as Unity Catalog schemas in the `workspace` catalog.
- All tables are stored as Delta tables.
- Transformations are written in PySpark (Silver) and Spark SQL (Gold).
- Each layer is run by a notebook-based orchestration step.
- Loads are full refreshes (`overwrite`), not incremental.

## Architecture

Data moves through three layers:

1. **Bronze:** CSV files from `source_crm` and `source_erp` are read from a Unity Catalog Volume and written as Delta tables with no business transformation.
2. **Silver:** Each Bronze table is trimmed, standardised, validated and renamed to consistent column names.
3. **Gold:** Silver tables are joined into two dimensions and one fact table.

<img width="1888" height="1787" alt="databricks_project_architecture" src="https://github.com/user-attachments/assets/e5de3ac8-1d6a-4e2b-ba74-e13527bf1f83" />



## Tech Stack

| Technology | Role |
|---|---|
| Databricks | Execution platform: notebooks, compute, Repos |
| Apache Spark / PySpark | Ingestion and Silver-layer transformations |
| Spark SQL | Gold-layer modelling and validation queries |
| Delta Lake | Storage format for all layers |
| Unity Catalog | Catalog, schemas (`bronze`, `silver`, `gold`) and Volume for raw files |

## Project Structure

```text
databricks-data-lakehouse/
├── bike_lakehouse/
│   ├── init_lakehouse.ipynb          # Creates schemas and the Volume
│   ├── bronze/
│   │   └── bronze.ipynb              # Config-driven CSV → Delta ingestion (6 tables)
│   ├── silver/
│   │   ├── silver_crm_cust_info.ipynb
│   │   ├── silver_crm_prd_info.ipynb
│   │   ├── silver_crm_sales_details.ipynb
│   │   ├── silver_erp_cust_az12.ipynb
│   │   ├── silver_erp_loc_a101.ipynb
│   │   ├── silver_erp_px_cat_g1v2.ipynb
│   │   └── silver_orchestration.ipynb  # Runs all Silver notebooks
│   └── gold/
│       ├── gold_dim_customers.ipynb
│       ├── gold_dim_products.ipynb
│       ├── gold_fact_sales.ipynb
│       └── gold_orchestration.ipynb    # Runs Gold notebooks in dependency order
└── datasets/
    ├── source_crm/                   # cust_info, prd_info, sales_details (CSV)
    └── source_erp/                   # CUST_AZ12, LOC_A101, PX_CAT_G1V2 (CSV)
```

## Getting Started

### Prerequisites

- A free [Databricks Free Edition](https://www.databricks.com/learn/free-edition) account. No paid workspace, cloud subscription or cluster setup is required.

### Setup

1. **Import the repository.** In Databricks go to *Workspace → Repos → Add Repo* and paste the GitHub URL.
2. **Initialise the Lakehouse.** Run `bike_lakehouse/init_lakehouse.ipynb`. It selects the `workspace` catalog, creates the `bronze`, `silver` and `gold` schemas, and creates a Volume.
3. **Upload the source files.** Copy the contents of `datasets/` into the Volume so that the paths match the Bronze configuration:
   ```text
   /Volumes/workspace/bronze/source_systems/source_crm/*.csv
   /Volumes/workspace/bronze/source_systems/source_erp/*.csv
   ```
4. **Run the pipeline in order:**
   1. `bronze/bronze.ipynb`
   2. `silver/silver_orchestration.ipynb`
   3. `gold/gold_orchestration.ipynb`

### Configuration

Values are hard-coded in the notebooks. Change them if your environment differs.

| Setting | Current value | Location |
|---|---|---|
| Catalog | `workspace` | all notebooks |
| Schemas | `bronze`, `silver`, `gold` | `init_lakehouse.ipynb` |
| Source file paths | `/Volumes/workspace/bronze/source_systems/...` | `INGESTION_CONFIG` in `bronze.ipynb` |



## Data Pipeline

### Bronze: raw ingestion

- Each of the 6 CSV files is read with `header=true` and `inferSchema=true`.
- Each file is written as a Delta table in `workspace.bronze` with `overwrite` mode.
- The file-to-table mapping lives in the `INGESTION_CONFIG` list.
- Tables: `crm_cust_info`, `crm_prd_info`, `crm_sales_details`, `erp_cust_az12`, `erp_loc_a101`, `erp_px_cat_g1v2`.

### Silver: cleaning and standardisation

All Silver notebooks trim whitespace in string columns and rename columns to descriptive names.

| Silver table | Main transformations |
|---|---|
| `crm_customers` | Marital status `S`/`M` → `Single`/`Married`; gender `M`/`F` → `Male`/`Female`; unknown → `n/a`; rows with null `cst_id` removed |
| `crm_products` | Null cost → `0`; `prd_key` split into `category_id` (first 5 characters, `-` → `_`) and `product_number`; product line codes `M`/`R`/`T`/`S` → `Mountain`/`Road`/`Touring`/`Other Sales`; start date cast to `DATE` |
| `crm_sales` | `yyyyMMdd` integers converted to dates; zero or malformed dates → `NULL`; invalid price (null or ≤ 0) recomputed as `sales / quantity` |
| `erp_customers` | `NAS` prefix removed from customer ID; gender normalised; birth dates in the future set to `NULL` |
| `erp_customer_location` | `-` removed from customer ID; `US`/`USA` → `United States`; `DE` → `Germany`; empty → `n/a` |
| `erp_product_category` | `Yes`/`No` maintenance flag → boolean |

### Gold: star schema

| Table | Grain | Logic |
|---|---|---|
| `dim_customers` | One row per CRM customer | Joins CRM customers with ERP demographics and location on `customer_number`; surrogate `customer_key` via `ROW_NUMBER()`; CRM gender is used first, ERP gender as fallback |
| `dim_products` | One row per product record | Joins CRM products with ERP categories on `category_id`; surrogate `product_key`; historical records are retained |
| `fact_sales` | One row per order line | Joins Silver sales with both dimensions to attach `customer_key` and `product_key` |


## Example Queries

### Top countries by sales

```sql
SELECT
    c.country,
    COUNT(DISTINCT f.order_number) AS orders,
    SUM(f.sales_amount)            AS total_sales
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_customers c
    ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_sales DESC
LIMIT 10;
```

### Revenue by product category and line

```sql
SELECT
    p.category,
    p.product_line,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.quantity)     AS units_sold
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_products p
    ON f.product_key = p.product_key
GROUP BY p.category, p.product_line
ORDER BY total_sales DESC;
```






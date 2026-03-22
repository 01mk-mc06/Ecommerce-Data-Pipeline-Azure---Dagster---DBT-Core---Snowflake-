# E-Commerce Data Pipeline Project Documentation

**Project:** End-to-End Analytics Engineering Pipeline  
**Stack:** Azure Blob Storage → Snowflake → dbt → Dagster  
**Author:** King  
**Date:** March 2026  

---

## Table of Contents

1. [Project Overview]
2. [Architecture]
3. [Infrastructure Setup]
4. [Data Generation & Upload]
5. [dbt Staging Layer]
6. [dbt Marts Layer]
7. [Dagster Orchestration]
8. [Future Improvements]
9. [Lessons Learned]
10. [Appendix A: Snowflake DDL Scripts]
11. [Appendix B: Python Data Generator]
12. [Appendix C: dbt Code]
13. [Appendix D: Dagster Code]

---

## 1. Project Overview

### 1.1 Business Problem

A mid-sized e-commerce company receives daily CSV file dumps from their transactional systems into Azure Blob Storage. Current pain points:

- **Data Quality Issues:** Duplicate orders, missing customer IDs, future dates in order timestamps
- **Manual Processes:** Analysts manually upload CSVs to Snowflake and run ad-hoc SQL scripts
- **No Monitoring:** Pipelines fail silently, revenue reports show 15% inflation due to duplicates
- **Access Control Issues:** Junior analysts have ACCOUNTADMIN access, accidentally dropped production tables
- **No Lineage:** When dashboards break, no one knows which upstream data changed

### 1.2 Solution Overview

Build an automated, tested, and monitored data pipeline:

```
Azure Blob Storage (raw CSVs)
    ↓ (Snowpipe auto-ingestion)
Snowflake RAW_DB (untrusted data)
    ↓ (dbt transformations + data quality tests)
Snowflake ANALYTICS_DB (curated models)
    ↓ (Dagster orchestration + monitoring)
BI Tools / Analysts
```

### 1.3 Success Metrics

- ✅ **Zero manual data loading** (Snowpipe automation)
- ✅ **99% data quality SLA** (dbt tests catch bad data)
- ✅ **Complete audit trail** (every transformation documented)
- ✅ **Role-based access control** (analysts can't break engineering layer)
- ✅ **Automated monitoring** (Dagster alerts on failures)

### 1.4 Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Data Lake | Azure Blob Storage | Landing zone for CSV files |
| Data Warehouse | Snowflake | Storage + compute |
| Transformation | dbt Core | SQL-based modeling framework |
| Orchestration | Dagster | Workflow automation + monitoring |
| Version Control | Git | Code versioning |
| Languages | Python, SQL | Scripting + data modeling |

---

## 2. Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Azure Blob Storage                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                 │
│  │ orders/  │  │customers/│  │products/ │                 │
│  │ *.csv    │  │ *.csv    │  │ *.csv    │                 │
│  └──────────┘  └──────────┘  └──────────┘                 │
└─────────────────────┬───────────────────────────────────────┘
                      │ Snowpipe Auto-Ingest
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                    Snowflake RAW_DB                         │
│  ┌──────────────┐  ┌───────────────┐  ┌─────────────┐     │
│  │ raw_orders   │  │ raw_customers │  │raw_products │     │
│  │ (VARCHAR)    │  │ (VARCHAR)     │  │(VARCHAR)    │     │
│  └──────────────┘  └───────────────┘  └─────────────┘     │
└─────────────────────┬───────────────────────────────────────┘
                      │ dbt Staging Layer (data quality)
                      ↓
┌─────────────────────────────────────────────────────────────┐
│              Snowflake ANALYTICS_DB.STAGING                 │
│  ┌──────────────┐  ┌───────────────┐  ┌─────────────┐     │
│  │ stg_orders   │  │ stg_customers │  │stg_products │     │
│  │ (deduped,    │  │ (deduplicated,│  │(nulls       │     │
│  │  typed)      │  │  validated)   │  │ replaced)   │     │
│  └──────────────┘  └───────────────┘  └─────────────┘     │
└─────────────────────┬───────────────────────────────────────┘
                      │ dbt Marts Layer (business logic)
                      ↓
┌─────────────────────────────────────────────────────────────┐
│               Snowflake ANALYTICS_DB.MARTS                  │
│  ┌──────────────┐  ┌───────────────┐  ┌─────────────┐     │
│  │ fct_orders   │  │ dim_customers │  │dim_products │     │
│  │ (star schema)│  │ (SCD Type 1)  │  │(enriched)   │     │
│  └──────────────┘  └───────────────┘  └─────────────┘     │
└─────────────────────┬───────────────────────────────────────┘
                      │ Consumed by BI Tools
                      ↓
              Power BI / Tableau
```

### 2.2 Data Flow

1. **Ingestion:** Marketing team uploads CSVs to Azure Blob daily
2. **Auto-Load:** Snowpipe detects new files, loads to RAW tables
3. **Quality Check:** dbt staging models deduplicate, type-cast, filter bad data
4. **Transform:** dbt marts models apply business logic, create star schema
5. **Test:** dbt runs 40+ tests (uniqueness, nulls, relationships, business rules)
6. **Orchestrate:** Dagster monitors, schedules, alerts on failures
7. **Consume:** Analysts query marts layer via Power BI

### 2.3 Project Structure

```
ecom_pipeline_project/
├── .env                          # Environment variables (GITIGNORED)
├── run_dbt.py                    # Python wrapper for dbt commands
├── data_generator/
│   └── generate_ecom_data.py     # Fake data generator with quality issues
├── dbt_ecommerce/
│   ├── dbt_project.yml           # dbt project config
│   ├── profiles.yml              # Snowflake connection (in ~/.dbt/)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml       # Raw table definitions
│   │   │   ├── schema.yml        # Staging model tests
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_products.sql
│   │   └── marts/
│   │       ├── schema.yml        # Marts model tests
│   │       ├── fct_orders.sql
│   │       ├── dim_customers.sql
│   │       └── dim_products.sql
│   ├── macros/
│   │   └── generate_schema_name.sql  # Custom schema override
│   ├── tests/
│   │   └── assert_revenue_matches.sql  # Singular test
│   └── target/
│       └── manifest.json         # Generated for Dagster
└── dagster_ecommerce/
    ├── __init__.py               # Dagster definitions
    ├── assets/
    │   ├── azure_assets.py       # Azure Blob monitoring
    │   ├── snowpipe_assets.py    # Snowpipe refresh
    │   └── dbt_assets.py         # dbt model wrapper
    ├── resources/
    │   ├── dbt_resource.py       # dbt CLI resource
    │   ├── snowflake_resource.py # Snowflake connector
    │   └── azure_resource.py     # Azure Blob SDK
    ├── sensors/
    │   ├── file_sensor.py        # Azure file watcher
    │   └── alert_sensor.py       # Failure alerts
    ├── schedules/
    │   └── daily_schedule.py     # 2 AM daily run
    └── jobs/
        └── daily_job.py          # Asset job definitions
```

---

## 3. Part 1: Infrastructure Setup

### 3.1 Objectives

- Create Azure Blob Storage container for raw data
- Configure Snowflake warehouses, databases, schemas
- Implement role-based access control (RBAC)
- Set up Snowpipe for auto-ingestion
- Create local development environment

### 3.2 Azure Setup (Portal UI)

#### 3.2.1 Create Resource Group

1. Navigate to Azure Portal → Resource Groups → **+ Create**
2. **Resource group:** `rg-ecom-pipeline`
3. **Region:** `Southeast Asia`
4. Click **Review + Create** → **Create**

#### 3.2.2 Create Storage Account

1. Navigate to **Storage Accounts** → **+ Create**
2. **Resource group:** `rg-ecom-pipeline`
3. **Storage account name:** `stecomdataYYYYMMDD` (use current date, must be globally unique)
4. **Region:** `Southeast Asia`
5. **Performance:** `Standard`
6. **Redundancy:** `Locally-redundant storage (LRS)`
7. Click **Review + Create** → **Create**

#### 3.2.3 Create Container

1. Open storage account → **Containers** → **+ Container**
2. **Name:** `raw-data`
3. **Public access level:** `Private`
4. Click **Create**

#### 3.2.4 Create Folder Structure

Inside `raw-data` container:
- **orders/** (for orders_YYYYMMDD.csv files)
- **customers/** (for customers_YYYYMMDD.csv files)
- **products/** (for products_YYYYMMDD.csv files)

#### 3.2.5 Get Storage Account Keys

1. Storage account → **Security + networking** → **Access keys**
2. Click **Show** next to `key1`
3. **Copy and save:**
   - Storage account name: `stecomdataYYYYMMDD`
   - Key1 value: (long string - save to `.env` later)

#### 3.2.6 Create Service Principal

1. Navigate to **Microsoft Entra ID** → **App registrations** → **+ New registration**
2. **Name:** `sp-snowflake-integration`
3. **Supported account types:** `Accounts in this organizational directory only`
4. Click **Register**

5. **Copy and save:**
   - Application (client) ID: `<AZURE_CLIENT_ID>`
   - Directory (tenant) ID: `<AZURE_TENANT_ID>`

6. Go to **Certificates & secrets** → **+ New client secret**
   - **Description:** `Snowflake integration secret`
   - **Expires:** `24 months`
   - Click **Add**
   - **Copy the VALUE immediately** (can't view again later)

7. Assign role to service principal:
   - Go back to **Storage Account** → **Access Control (IAM)** → **+ Add** → **Add role assignment**
   - **Role:** `Storage Blob Data Contributor`
   - **Assign access to:** `User, group, or service principal`
   - Click **+ Select members** → Search `sp-snowflake-integration`
   - Click **Review + assign** → **Assign**

### 3.3 Snowflake Setup

See [Appendix A: Snowflake DDL Scripts](#10-appendix-a-snowflake-ddl-scripts) for full SQL code.

#### 3.3.1 Create Snowflake Trial Account

1. Visit https://signup.snowflake.com/
2. **Cloud Provider:** Microsoft Azure
3. **Region:** Southeast Asia (Singapore)
4. **Edition:** Standard
5. Save credentials:
   - **Account URL:** `https://<ACCOUNT_IDENTIFIER>.southeast-asia.azure.snowflakecomputing.com`
   - **Username:** (your email)
   - **Password:** (set during signup)

#### 3.3.2 Snowflake Objects Created

**Warehouses:**
- `ecom_loading_wh` (X-Small, for Snowpipe)
- `ecom_transform_wh` (Small, for dbt)
- `ecom_analyst_wh` (X-Small, for BI queries)

**Databases & Schemas:**
- `ecom_raw_db.ecom_raw_schema` (landing zone)
- `ecom_analytics_db.ecom_staging` (cleaned data)
- `ecom_analytics_db.ecom_marts` (business models)

**Roles:**
- `engineer_role` (can transform data, run dbt)
- `analyst_role` (read-only access to marts)

**Tables:**
- `raw_orders`, `raw_customers`, `raw_products` (VARCHAR columns, loaded by Snowpipe)

**Snowpipes:**
- `pipe_orders`, `pipe_customers`, `pipe_products` (auto-ingest from Azure Blob)

### 3.4 Local Environment Setup

#### 3.4.1 Create Project Directory

```
ecom_pipeline_project/
├── data_generator/
├── dbt_ecommerce/
├── dagster_ecommerce/
└── venv/
```

#### 3.4.2 Python Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Mac/Linux)
source venv/bin/activate

# Install packages
pip install dbt-snowflake dagster dagster-webserver dagster-dbt \
    dagster-snowflake azure-storage-blob python-dotenv faker pandas \
    snowflake-connector-python
```

#### 3.4.3 Create .env File

Create `.env` in project root:

```bash
# Azure Storage
AZURE_STORAGE_ACCOUNT_NAME=stecomdataYYYYMMDD
AZURE_STORAGE_ACCOUNT_KEY=<your_storage_key_here>
AZURE_CONTAINER_NAME=raw-data

# Snowflake
SNOWFLAKE_ACCOUNT=<ACCOUNT_IDENTIFIER>.southeast-asia.azure
SNOWFLAKE_USER=<your_email@example.com>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ROLE=accountadmin
SNOWFLAKE_WAREHOUSE=ecom_transform_wh
SNOWFLAKE_DATABASE=ecom_analytics_db
SNOWFLAKE_SCHEMA=ecom_staging
```

**⚠️ IMPORTANT:** Add `.env` to `.gitignore` immediately!

#### 3.4.4 Create dbt Profile

Create `~/.dbt/profiles.yml`:

```yaml
dbt_ecommerce:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      database: ecom_analytics_db
      schema: ecom_staging
      threads: 4
      client_session_keep_alive: false
```

### 3.5 Challenges & Solutions

#### Challenge 1: Schema Naming Conflicts

**Problem:** dbt was creating schemas like `ecom_staging_ecom_staging` (double prefixing)

**Root Cause:** dbt appends custom schema to target schema by default

**Solution:** Created custom `generate_schema_name` macro

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}
```

**Lesson Learned:** Always check compiled SQL in `target/` folder to debug schema issues

#### Challenge 2: Azure Container Name Invalid Characters

**Problem:** `InvalidResourceName` error when uploading to Azure

**Root Cause:** Used `raw_data` (underscore) instead of `raw-data` (hyphen)

**Solution:** Azure Blob container names only allow lowercase letters, numbers, and hyphens

**Troubleshooting Steps:**
1. Checked Azure Portal for actual container name
2. Updated `.env` file to match
3. Deactivated/reactivated venv to reload environment variables

#### Challenge 3: Environment Variables Not Loading

**Problem:** dbt couldn't find Snowflake credentials even though `.env` existed

**Root Cause:** dbt doesn't automatically load `.env` files

**Solution:** Created `run_dbt.py` wrapper to load dotenv before running dbt

```python
# run_dbt.py
import os
import sys
from dotenv import load_dotenv
import subprocess

load_dotenv()

# Verify variables loaded
required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
missing = [v for v in required_vars if not os.getenv(v)]
if missing:
    print(f"❌ Missing: {', '.join(missing)}")
    sys.exit(1)

dbt_command = ' '.join(sys.argv[1:]) if len(sys.argv) > 1 else 'debug'
os.chdir('dbt_ecommerce')
result = subprocess.run(f"dbt {dbt_command}", shell=True)
sys.exit(result.returncode)
```

Usage: `python run_dbt.py run --select staging`

---

## 4. Part 2: Data Generation & Upload

### 4.1 Objectives

- Generate realistic e-commerce data (1500 orders, 300 customers, 50 products)
- Inject intentional data quality issues (duplicates, nulls, future dates)
- Upload CSVs to Azure Blob Storage
- Trigger Snowpipe to load raw data into Snowflake

### 4.2 Data Generator Script

See [Appendix B: Python Data Generator](#11-appendix-b-python-data-generator) for full code.

**Data Quality Issues Injected:**
- **Duplicate order_ids:** ~7% (105 duplicates)
- **NULL customer_id:** ~4% (60 rows)
- **NULL product_id:** ~2% (30 rows)
- **Future dates:** ~3% (45 orders with `order_date > current_date`)
- **NULL customer names:** ~2% (6 rows)
- **NULL phone numbers:** ~8% (24 rows)
- **Duplicate emails:** ~5% (15 duplicates)

**Why Inject Bad Data?**
- Tests dbt's ability to catch quality issues
- Demonstrates value of data quality gates
- Shows before/after data cleansing impact

### 4.3 Upload to Azure Blob

```bash
python data_generator/generate_ecom_data.py
```

**Expected Output:**
```
🔄 Generating e-commerce data...

📊 Data Summary:
  Products: 50 rows
  Customers: 300 rows
  Orders: 1500 rows

⚠️  Intentional Data Quality Issues:
  Products with NULL category: 6
  Products with NULL brand: 9
  Products with NULL cost: 1
  Customers with NULL name: 8
  Customers with NULL phone: 17
  Duplicate customer emails: 12
  Orders with NULL customer_id: 55
  Orders with NULL product_id: 23
  Duplicate order_ids: 110
  Orders with future dates: 46

☁️  Uploading to Azure Blob Storage...
✅ Uploaded products/products_20260322.csv (50 rows)
✅ Uploaded customers/customers_20260322.csv (300 rows)
✅ Uploaded orders/orders_20260322.csv (1500 rows)

✨ Data generation complete!
```

### 4.4 Trigger Snowpipe

Since `AUTO_INGEST=FALSE` (manual mode for learning), trigger pipes manually.

See [Appendix A: Snowflake - Trigger Snowpipe](#snowflake-trigger-snowpipe) for SQL commands.

**Verification Query:**

```sql
-- Check row counts
SELECT 'raw_orders' as table_name, COUNT(*) as row_count FROM ecom_raw_db.ecom_raw_schema.raw_orders
UNION ALL
SELECT 'raw_customers', COUNT(*) FROM ecom_raw_db.ecom_raw_schema.raw_customers
UNION ALL
SELECT 'raw_products', COUNT(*) FROM ecom_raw_db.ecom_raw_schema.raw_products;

-- Expected: 1500, 300, 50
```

### 4.5 Challenges & Solutions

#### Challenge 1: Missing Python Packages

**Problem:** `ModuleNotFoundError: No module named 'faker'`

**Root Cause:** Didn't install all dependencies initially

**Solution:**
```bash
pip install faker pandas azure-storage-blob
```

**Lesson Learned:** Create `requirements.txt` at project start to track dependencies

#### Challenge 2: Azure Upload Timeout

**Problem:** Uploads failed intermittently with timeout errors

**Root Cause:** Network latency to Southeast Asia region

**Workaround:** Added retry logic in upload function

```python
from azure.core.exceptions import ServiceRequestError
import time

def upload_to_azure(df, folder, filename, max_retries=3):
    for attempt in range(max_retries):
        try:
            # ... upload logic ...
            break
        except ServiceRequestError:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            raise
```

---

## 5. Part 3: dbt Staging Layer

### 5.1 Objectives

- Define source tables in `sources.yml`
- Create staging models to clean raw data
- Implement data quality tests (unique, not_null, relationships)
- Remove duplicates, filter bad data, type-cast columns

### 5.2 dbt Project Initialization

```bash
# Initialize dbt project (already done)
cd ecom_pipeline_project
dbt init dbt_ecommerce

# Test connection
python run_dbt.py debug
# Should show: "Connection test: [OK connection ok]"
```

### 5.3 Project Configuration

**dbt_project.yml:**

```yaml
name: 'dbt_ecommerce'
version: '1.0.0'
profile: 'dbt_ecommerce'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

models:
  dbt_ecommerce:
    staging:
      +materialized: view
    marts:
      +materialized: table
      +schema: ecom_marts
```

### 5.4 Source Definitions

See [Appendix C: dbt - sources.yml](#dbt-sourcesyml) for full code.

**Key Features:**
- Freshness checks (warn after 24 hours, error after 48 hours)
- Loaded_at field for timestamp tracking
- Column-level descriptions

### 5.5 Staging Models

#### 5.5.1 stg_orders.sql

See [Appendix C: dbt - stg_orders.sql](#dbt-stg_orderssql) for full code.

**Transformations:**
- ✅ Type-cast VARCHAR to proper types (TIMESTAMP, NUMBER)
- ✅ Remove duplicates using `ROW_NUMBER()` window function
- ✅ Filter NULL customer_id, product_id
- ✅ Filter future dates (`order_date > CURRENT_TIMESTAMP()`)
- ✅ Calculate `total_amount = quantity * unit_price`
- ✅ Standardize `order_status` to lowercase

**Data Quality Impact:**
```
Raw:     1500 rows
Staging: ~1290 rows (cleaned)

Removed:
- 110 duplicate order_ids
- 55 NULL customer_id
- 23 NULL product_id  
- 46 future dates
≈ 210 rows removed (14% of data)
```

#### 5.5.2 stg_customers.sql

See [Appendix C: dbt - stg_customers.sql](#dbt-stg_customerssql) for full code.

**Transformations:**
- ✅ Replace NULL names with 'Unknown'
- ✅ Replace NULL phones with 'N/A'
- ✅ Lowercase and trim email addresses
- ✅ Deduplicate by customer_id (keep most recent)
- ✅ Type-cast registration_date to DATE

#### 5.5.3 stg_products.sql

See [Appendix C: dbt - stg_products.sql](#dbt-stg_productssql) for full code.

**Transformations:**
- ✅ Replace NULL category with 'Uncategorized'
- ✅ Replace NULL brand with 'Generic'
- ✅ Type-cast cost and price to NUMBER
- ✅ Calculate margin_percent: `((price - cost) / cost) * 100`

### 5.6 Schema Tests

See [Appendix C: dbt - staging/schema.yml](#dbt-stagingschemayml) for full code.

**Tests Applied:**
- `unique` on primary keys (order_id, customer_id, product_id)
- `not_null` on critical fields
- `relationships` (foreign key validation)
- `accepted_values` for enums (order_status)

**Test Execution:**

```bash
# Run staging models
python run_dbt.py run --select staging

# Run tests
python run_dbt.py test --select staging
```

**Expected Results:**
```
Completed successfully
Done. PASS=3 WARN=0 ERROR=0 SKIP=0 TOTAL=3  (models)
Done. PASS=16 WARN=0 ERROR=0 SKIP=0 TOTAL=16 (tests)
```

### 5.7 Challenges & Solutions

#### Challenge 1: Models Created in Wrong Schema

**Problem:** Staging models materialized in `ecom_staging_ecom_staging`

**Root Cause:** dbt default behavior appends custom schema to target schema

**Solution:** Created `macros/generate_schema_name.sql` (see Part 1)

**Verification:**
```sql
SHOW SCHEMAS IN DATABASE ecom_analytics_db;
-- Should only show: ECOM_STAGING, ECOM_MARTS
```

#### Challenge 2: Relationship Tests Failing

**Problem:** `relationships` test failed between `stg_orders` and `stg_customers`

**Root Cause:** Staging models ran in parallel, dim_customers didn't exist yet when fct_orders referenced it

**Solution:** Run staging first, then marts:

```bash
python run_dbt.py run --select staging  # First
python run_dbt.py run --select marts    # Then
```

**Better Solution:** Use dbt's dependency graph (already automatic with `{{ ref() }}`)

#### Challenge 3: Test Execution Slow

**Problem:** Running all tests took 3+ minutes

**Root Cause:** Each test runs as separate Snowflake query

**Optimization:**
1. Increase warehouse size for test runs: `TRANSFORM_WH` from X-Small to Small
2. Use `--fail-fast` flag to stop on first failure (during development)
3. Store test failures for analysis: `--store-failures`

```bash
python run_dbt.py test --fail-fast --store-failures
```

---

## 6. Part 4: dbt Marts Layer

### 6.1 Objectives

- Create star schema (fact + dimension tables)
- Apply business logic (customer tiers, product status)
- Add time dimensions (year, month, quarter)
- Calculate derived metrics (lifetime value, gross profit)
- Implement singular tests for cross-model validation

### 6.2 Dimension Tables

#### 6.2.1 dim_customers

See [Appendix C: dbt - dim_customers.sql](#dbt-dim_customerssql) for full code.

**Enrichments:**
- Customer lifetime value (sum of delivered orders)
- Total order count
- First/last order dates
- Customer tier (VIP/Gold/Silver/Bronze based on LTV)
- Customer status (Active/At Risk/Churned/New based on recency)

**Business Rules:**
```sql
CASE
  WHEN lifetime_value >= 1000 THEN 'VIP'
  WHEN lifetime_value >= 500 THEN 'Gold'
  WHEN lifetime_value >= 100 THEN 'Silver'
  ELSE 'Bronze'
END as customer_tier

CASE
  WHEN last_order_date >= DATEADD(day, -30, CURRENT_DATE) THEN 'Active'
  WHEN last_order_date >= DATEADD(day, -90, CURRENT_DATE) THEN 'At Risk'
  WHEN last_order_date IS NOT NULL THEN 'Churned'
  ELSE 'New'
END as customer_status
```

#### 6.2.2 dim_products

See [Appendix C: dbt - dim_products.sql](#dbt-dim_productssql) for full code.

**Enrichments:**
- Total orders, quantity sold, revenue
- Average selling price
- First/last sold dates
- Product status (Never Sold/Active/Slow Moving/Discontinued)

### 6.3 Fact Table

#### 6.3.1 fct_orders

See [Appendix C: dbt - fct_orders.sql](#dbt-fct_orderssql) for full code.

**Grain:** One row per order

**Measures:**
- `total_amount` (quantity × unit_price)
- `total_cost` (quantity × product_cost)
- `gross_profit` (total_amount - total_cost)

**Dimensions:**
- Customer attributes (tier, status)
- Product attributes (category, brand)
- Time dimensions (year, month, day, quarter, day_of_week)

**Flags:**
- `is_delivered` (1 if status = 'delivered', else 0)
- `is_cancelled` (1 if status = 'cancelled', else 0)

### 6.4 Marts Schema Tests

See [Appendix C: dbt - marts/schema.yml](#dbt-martsschemayml) for full code.

**Additional Tests:**
- Foreign key relationships (fct_orders → dim_customers, dim_products)
- Accepted values for tier and status enums
- Not null on calculated fields

### 6.5 Singular Tests

#### 6.5.1 Revenue Reconciliation

See [Appendix C: dbt - assert_revenue_matches.sql](#dbt-assert_revenue_matchessql) for full code.

**Purpose:** Ensure `fct_orders` revenue matches `stg_orders` (no data loss in transformation)

**Logic:**
```sql
ABS(staging_total - marts_total) > 0.01  -- Fail if difference > 1 cent
OR staging_count != marts_count          -- Fail if row counts differ
```

### 6.6 Execution

```bash
# Run marts models
python run_dbt.py run --select marts

# Run all tests
python run_dbt.py test

# Generate documentation
python run_dbt.py docs generate
python run_dbt.py docs serve  # Opens http://localhost:8080
```

**Results:**
```
Models:
  PASS=3 (dim_customers, dim_products, fct_orders)

Tests:
  PASS=21 (16 staging + 5 marts tests)
```

### 6.7 Challenges & Solutions

#### Challenge 1: Circular Dependency

**Problem:** `fct_orders` referenced `dim_customers` which referenced `fct_orders` (for LTV calculation)

**Root Cause:** Used `{{ ref('fct_orders') }}` inside `dim_customers` for aggregation

**Solution:** Calculate customer metrics directly from `stg_orders` instead

**Before (BAD):**
```sql
-- dim_customers.sql
WITH customer_orders AS (
  SELECT customer_id, SUM(total_amount) as ltv
  FROM {{ ref('fct_orders') }}  -- Creates circular dependency!
  GROUP BY customer_id
)
```

**After (GOOD):**
```sql
-- dim_customers.sql
WITH customer_orders AS (
  SELECT customer_id, SUM(total_amount) as ltv
  FROM {{ ref('stg_orders') }}  -- Use staging layer
  GROUP BY customer_id
)
```

**Lesson Learned:** Always reference upstream layers; avoid cross-mart dependencies

#### Challenge 2: NULL Handling in Calculations

**Problem:** `gross_profit` showing NULL when product_cost was NULL

**Root Cause:** `NULL - any_value = NULL` in SQL

**Solution:** Use `COALESCE()` to default NULL costs to 0

```sql
ROUND(o.total_amount - (o.quantity * COALESCE(p.cost, 0)), 2) as gross_profit
```

**Trade-off:** This assumes 0 cost for products missing cost data (acceptable for analytics; document assumption)

#### Challenge 3: Test Failures Due to Timing

**Problem:** `assert_revenue_matches` test failed intermittently

**Root Cause:** Test ran before `fct_orders` finished materializing

**Solution:** dbt automatically handles dependencies via `{{ ref() }}`, but ensure models run sequentially:

```bash
# Run models first, then tests
python run_dbt.py build  # Runs models + tests in correct order
```

---

## 7. Part 5: Dagster Orchestration

### 7.1 Objectives

- Wrap dbt models as Dagster assets
- Create file sensor to watch Azure Blob
- Automate Snowpipe refresh
- Schedule daily pipeline runs
- Add failure alerting
- Visualize asset lineage graph

### 7.2 Project Structure

```
dagster_ecommerce/
├── __init__.py               # Definitions (assets, sensors, schedules)
├── assets/
│   ├── azure_assets.py       # Azure Blob file monitoring
│   ├── snowpipe_assets.py    # Snowpipe refresh trigger
│   └── dbt_assets.py         # dbt model wrapper
├── resources/
│   ├── dbt_resource.py       # dbt CLI configuration
│   ├── snowflake_resource.py # Snowflake connector
│   └── azure_resource.py     # Azure Blob SDK client
├── sensors/
│   ├── file_sensor.py        # Detects new CSV files
│   └── alert_sensor.py       # Failure notifications
├── schedules/
│   └── daily_schedule.py     # Cron schedule (2 AM daily)
└── jobs/
    └── daily_job.py          # Asset job definitions
```

### 7.3 Resources (Connection Managers)

See [Appendix D: Dagster - Resources](#dagster-resources) for full code.

**Resources Created:**
- `dbt_resource` - Configures dbt CLI with project directory
- `snowflake_resource` - Snowflake connector for running queries
- `azure_resource` - Azure Blob client for listing files

### 7.4 Assets

#### 7.4.1 Azure Blob Asset

See [Appendix D: Dagster - azure_assets.py](#dagster-azure_assetspy) for full code.

**Purpose:** Check Azure Blob for available files

**Output:** Dictionary with file counts per folder

```python
{
  "orders": 1,
  "customers": 1,
  "products": 1
}
```

#### 7.4.2 Snowpipe Refresh Asset

See [Appendix D: Dagster - snowpipe_assets.py](#dagster-snowpipe_assetspy) for full code.

**Purpose:** Manually trigger Snowpipe to load new files

**Dependencies:** Runs after `azure_blob_asset`

**SQL Executed:**
```sql
ALTER PIPE ecom_raw_db.ecom_raw_schema.pipe_orders REFRESH;
ALTER PIPE ecom_raw_db.ecom_raw_schema.pipe_customers REFRESH;
ALTER PIPE ecom_raw_db.ecom_raw_schema.pipe_products REFRESH;
```

#### 7.4.3 dbt Assets

See [Appendix D: Dagster - dbt_assets.py](#dagster-dbt_assetspy) for full code.

**Purpose:** Wrap all dbt models as Dagster assets

**Magic:** Dagster reads `manifest.json` to create individual assets for each model with correct dependencies

**Dependencies:** Runs after `snowpipe_refresh`

### 7.5 Sensors

#### 7.5.1 File Sensor

See [Appendix D: Dagster - file_sensor.py](#dagster-file_sensorrpy) for full code.

**Purpose:** Wake up every 5 minutes, check if new files exist in Azure Blob

**Logic:**
1. Count total files in `orders/`, `customers/`, `products/`
2. Compare to last known count (stored in cursor)
3. If count increased → trigger pipeline
4. Update cursor with new count

**Frequency:** Every 5 minutes (`minimum_interval_seconds=300`)

#### 7.5.2 Alert Sensor

See [Appendix D: Dagster - alert_sensor.py](#dagster-alert_sensorrpy) for full code.

**Purpose:** Detect pipeline failures, log alerts

**Trigger:** Runs whenever any Dagster run fails

**Output:** Logs error details (could integrate with Slack/email)

### 7.6 Schedules

See [Appendix D: Dagster - daily_schedule.py](#dagster-daily_schedulerpy) for full code.

**Cron Expression:** `0 2 * * *` (every day at 2 AM)

**What It Runs:**
1. Check Azure Blob
2. Refresh Snowpipes
3. Run all dbt models (staging → marts)
4. Run all dbt tests

### 7.7 Jobs

See [Appendix D: Dagster - daily_job.py](#dagster-daily_jobpy) for full code.

**Jobs Defined:**
- `all_assets_job` - Runs every asset (used by schedule)
- `dbt_only_job` - Runs only dbt models (for quick refreshes)

### 7.8 Dagster Definitions

See [Appendix D: Dagster - __init__.py](#dagster-__init__py) for full code.

**Connects Everything:**
- Registers assets, sensors, schedules, jobs
- Injects resources into assets

### 7.9 Execution

```bash
# Generate dbt manifest (required for Dagster)
python run_dbt.py compile

# Start Dagster UI
dagster dev -m dagster_ecommerce
# Opens http://127.0.0.1:3000
```

**In Dagster UI:**
1. **Assets** tab → See lineage graph (Azure Blob → Snowpipe → dbt models)
2. **Materialize all** → Runs entire pipeline
3. **Automation** tab → Enable sensors and schedules
4. **Runs** tab → View execution history, logs

### 7.10 Challenges & Solutions

#### Challenge 1: Dagster Can't Find dbt Manifest

**Problem:** `FileNotFoundError: manifest.json does not exist`

**Root Cause:** Need to run `dbt compile` before starting Dagster

**Solution:**
```bash
python run_dbt.py compile  # Generates target/manifest.json
dagster dev -m dagster_ecommerce
```

**Lesson Learned:** Add `dbt compile` to deployment checklist

#### Challenge 2: dbt_assets() Keyword Argument Error

**Problem:** `TypeError: dbt_assets() got an unexpected keyword argument 'project_dir'`

**Root Cause:** `dagster-dbt` API changed in v0.23+, removed `project_dir` parameter

**Solution:** Remove `project_dir` from `@dbt_assets` decorator

**Before:**
```python
@dbt_assets(
    manifest=MANIFEST_PATH,
    project_dir=DBT_PROJECT_DIR  # ❌ Not supported
)
```

**After:**
```python
@dbt_assets(
    manifest=MANIFEST_PATH  # ✅ Correct
)
```

#### Challenge 3: Sensor Not Triggering

**Problem:** File sensor showed "Sensor has never run" even though enabled

**Root Cause:** Sensor interval set to 5 minutes, hadn't ticked yet

**Troubleshooting:**
1. Click **Test Sensor** button in UI (manual trigger)
2. Check **Recent ticks** section for evaluation results
3. Verify cursor logic (print statements in sensor code)

**Solution:** Wait 5 minutes OR reduce `minimum_interval_seconds=60` for testing

#### Challenge 4: Environment Variables Not Available in Dagster

**Problem:** Resources couldn't find `SNOWFLAKE_ACCOUNT` from `.env`

**Root Cause:** Dagster daemon doesn't inherit terminal environment

**Solution:** Dagster automatically loads `.env` from project root (verified in startup logs)

**Verification:**
```
2026-03-22 17:38:16 - dagster-webserver - INFO - Loaded environment variables from .env file: 
AZURE_STORAGE_ACCOUNT_NAME, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER...
```

---

## 8. Future Improvements

### Part 6: Security & Governance (Planned)

**Objectives:**
- Implement column-level masking (email, phone, revenue)
- Add row-level security (region-based access)
- Create tag-based data classification (PII/SENSITIVE/PUBLIC)
- Enable audit logging (track who accessed what)
- Set up dynamic data masking (different data per role)

**Technologies:**
- Snowflake masking policies
- Snowflake row access policies
- Snowflake tag-based governance
- Snowflake QUERY_HISTORY views

**Expected Outcomes:**
- Analysts see masked emails (`jo***@gmail.com`)
- Analysts see NULL for revenue columns
- Engineers see full data
- Compliance-ready audit trail

### Part 7: Comprehensive dbt Testing (Planned)

**Objectives:**
- Install `dbt-expectations` package (Great Expectations integration)
- Create 5+ custom generic tests (email format, date ranges, outliers)
- Add 5+ singular tests (cross-model reconciliation)
- Implement test severity levels (warn vs error)
- Add statistical tests (mean, std dev, quantile checks)

**Technologies:**
- dbt-expectations package
- dbt test configurations
- dbt test macros

**Expected Outcomes:**
- 50+ total tests across all models
- Catch 99% of data quality issues before BI layer
- Test documentation in dbt docs

### Part 8: SCD Type 2 (Planned)

**Objectives:**
- Track customer tier changes over time
- Track product price history with valid_from/valid_to
- Implement dbt snapshots for historical tracking
- Enable point-in-time analysis queries

**Technologies:**
- dbt snapshots (timestamp strategy)
- Snowflake time-travel
- dbt snapshot tests

**Use Cases:**
- "What was this customer's tier on Dec 1, 2025?"
- "How has product pricing changed over 6 months?"
- "When did customer churn (tier dropped from Gold to Bronze)?"

### Additional Enhancements

**CI/CD Pipeline:**
- GitHub Actions for dbt Slim CI
- Run only changed models on PR
- Automated deployment to prod

**Data Quality Monitoring:**
- dbt test result tracking over time
- Trend analysis (are tests failing more frequently?)
- Slack alerts for test failures

**Performance Optimization:**
- Incremental models for large fact tables
- Clustering keys on high-cardinality columns
- Materialized views for frequently queried aggregations

**Observability:**
- dbt model execution time tracking
- Snowflake query cost monitoring
- Dagster asset metadata (row counts, execution duration)

---

## 9. Lessons Learned

### 9.1 Technical Lessons

#### Lesson 1: Schema Management is Critical

**What Happened:** Spent 2 hours debugging why models materialized in wrong schemas

**Root Cause:** Didn't understand dbt's default schema behavior (appends custom to target)

**Takeaway:** Always check `target/compiled/` SQL to verify schema logic before running

**Best Practice:**
```yaml
# dbt_project.yml - Be explicit about schemas
models:
  staging:
    +schema: staging  # Creates target.staging
  marts:
    +schema: marts    # Creates target.marts
```

#### Lesson 2: Environment Variables Need Explicit Loading

**What Happened:** dbt couldn't find credentials even though `.env` existed

**Root Cause:** dbt doesn't auto-load `.env`; need `python-dotenv` wrapper

**Takeaway:** Don't assume tools auto-load environment files

**Best Practice:** Create wrapper scripts (`run_dbt.py`) to handle env loading

#### Lesson 3: Test Failures Are Expected (And Valuable)

**What Happened:** Initial test run showed 8 failures (relationship tests, accepted_values)

**Root Cause:** Test ran before dependent models finished

**Takeaway:** Failures during development are normal; fix root cause, don't disable tests

**Best Practice:** Use `dbt build` (runs models + tests in dependency order)

#### Lesson 4: Dagster Reads Manifest, Not Live dbt State

**What Happened:** Changed dbt model, Dagster didn't reflect changes

**Root Cause:** Forgot to recompile manifest after dbt changes

**Takeaway:** Always run `dbt compile` after modifying models

**Best Practice:** Add to deployment checklist:
```bash
1. Make dbt changes
2. python run_dbt.py compile
3. Restart Dagster
```

#### Lesson 5: Credentials in Code = Bad; .env = Good

**What Happened:** Almost committed Azure storage key to Git

**Root Cause:** Hardcoded credentials in Python script for testing

**Takeaway:** NEVER hardcode credentials, even temporarily

**Best Practice:**
```bash
# Immediately after project creation:
echo ".env" >> .gitignore
git add .gitignore
git commit -m "Add gitignore"
```

### 9.2 Process Lessons

#### Lesson 1: Start with Bad Data Intentionally

**What Happened:** Generated clean data first, then added dbt tests

**Problem:** Tests passed but provided no value (no quality issues to catch)

**Better Approach:** Inject bad data → build tests that catch it → see tests work

**Benefit:** Demonstrates ROI of data quality framework

#### Lesson 2: Documentation as You Build, Not After

**What Happened:** Had to reverse-engineer decisions after 3 days of coding

**Problem:** Forgot why certain design choices were made

**Better Approach:** Add `description` and `meta` fields in `schema.yml` while building models

**Example:**
```yaml
- name: dim_customers
  description: |
    Customer dimension with lifetime metrics.
    
    Business Logic:
    - Customer tier based on LTV (VIP: $1000+, Gold: $500+, Silver: $100+, Bronze: <$100)
    - Status based on recency (Active: <30 days, At Risk: 30-90 days, Churned: >90 days)
    
    Data Quality:
    - Deduped by customer_id (most recent record wins)
    - NULL names replaced with 'Unknown'
```

#### Lesson 3: Version Control Everything (Except Credentials)

**What Happened:** Lost 1 hour of work when VM crashed

**Problem:** Didn't commit changes to Git frequently

**Better Approach:** Commit after every working feature

**Git Workflow:**
```bash
# After each Part completion
git add .
git commit -m "Part 3: dbt staging layer complete"
git push origin main
```

### 9.3 Design Lessons

#### Lesson 1: Staging Layer Should Be Thin

**What Happened:** Initially put business logic (customer tiers) in staging

**Problem:** Staging became complicated, hard to test

**Better Approach:**
- **Staging:** Clean, dedupe, type-cast (technical transformations)
- **Marts:** Business logic, aggregations, denormalization

**Benefit:** Staging models are reusable; marts can evolve independently

#### Lesson 2: Separate Read/Write Roles in Snowflake

**What Happened:** Used `ACCOUNTADMIN` for everything during development

**Problem:** No guard rails; easy to accidentally drop tables

**Better Approach:** Create granular roles from day 1

**Example:**
```sql
-- Engineering (can create objects)
GRANT CREATE TABLE ON SCHEMA analytics_db.marts TO ROLE engineer_role;

-- Analytics (read-only)
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_db.marts TO ROLE analyst_role;
```

#### Lesson 3: Sensors + Schedules Serve Different Purposes

**What Happened:** Initially used only schedule (2 AM daily)

**Problem:** If files arrived at 3 PM, had to wait 23 hours for processing

**Better Approach:** Use both
- **Sensor:** Trigger on file arrival (real-time processing)
- **Schedule:** Backup daily run (ensures pipeline runs even if no new files)

---

## 10. Appendix A: Snowflake DDL Scripts

### Snowflake: Create Warehouses

```sql
-- ============================================
-- 1. CREATE WAREHOUSES
-- ============================================
USE ROLE ACCOUNTADMIN;

-- Warehouse for Snowpipe (auto-ingestion)
CREATE WAREHOUSE ecom_loading_wh
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'For Snowpipe auto-loading from Azure Blob';

-- Warehouse for dbt transformations
CREATE WAREHOUSE ecom_transform_wh
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'For dbt model runs';

-- Warehouse for analyst queries
CREATE WAREHOUSE ecom_analyst_wh
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'For BI tool queries';
```

### Snowflake: Create Databases & Schemas

```sql
-- ============================================
-- 2. CREATE DATABASES & SCHEMAS
-- ============================================

CREATE DATABASE ecom_raw_db
  COMMENT = 'Untrusted data from source systems';

CREATE SCHEMA ecom_raw_db.ecom_raw_schema
  COMMENT = 'E-commerce source data';

CREATE DATABASE ecom_analytics_db
  COMMENT = 'dbt-managed curated data models';

CREATE SCHEMA ecom_analytics_db.ecom_staging
  COMMENT = 'Cleaned and deduped staging models';

CREATE SCHEMA ecom_analytics_db.ecom_marts
  COMMENT = 'Business-ready fact and dimension tables';
```

### Snowflake: Create Roles & Grants

```sql
-- ============================================
-- 3. CREATE ROLES & GRANTS
-- ============================================

CREATE ROLE engineer_role
  COMMENT = 'Analytics Engineers - can build models';

GRANT USAGE ON WAREHOUSE ecom_transform_wh TO ROLE engineer_role;
GRANT USAGE ON DATABASE ecom_raw_db TO ROLE engineer_role;
GRANT USAGE ON SCHEMA ecom_raw_db.ecom_raw_schema TO ROLE engineer_role;
GRANT SELECT ON ALL TABLES IN SCHEMA ecom_raw_db.ecom_raw_schema TO ROLE engineer_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ecom_raw_db.ecom_raw_schema TO ROLE engineer_role;

GRANT USAGE ON DATABASE ecom_analytics_db TO ROLE engineer_role;
GRANT ALL ON SCHEMA ecom_analytics_db.ecom_staging TO ROLE engineer_role;
GRANT ALL ON SCHEMA ecom_analytics_db.ecom_marts TO ROLE engineer_role;
GRANT ALL ON ALL TABLES IN SCHEMA ecom_analytics_db.ecom_staging TO ROLE engineer_role;
GRANT ALL ON ALL TABLES IN SCHEMA ecom_analytics_db.ecom_marts TO ROLE engineer_role;
GRANT ALL ON FUTURE TABLES IN SCHEMA ecom_analytics_db.ecom_staging TO ROLE engineer_role;
GRANT ALL ON FUTURE TABLES IN SCHEMA ecom_analytics_db.ecom_marts TO ROLE engineer_role;

CREATE ROLE analyst_role
  COMMENT = 'Business Analysts - read-only access to marts';

GRANT USAGE ON WAREHOUSE ecom_analyst_wh TO ROLE analyst_role;
GRANT USAGE ON DATABASE ecom_analytics_db TO ROLE analyst_role;
GRANT USAGE ON SCHEMA ecom_analytics_db.ecom_marts TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA ecom_analytics_db.ecom_marts TO ROLE analyst_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ecom_analytics_db.ecom_marts TO ROLE analyst_role;

-- Grant roles to your user (replace <YOUR_EMAIL>)
GRANT ROLE engineer_role TO USER <YOUR_EMAIL>;
GRANT ROLE analyst_role TO USER <YOUR_EMAIL>;

ALTER USER <YOUR_EMAIL> SET DEFAULT_ROLE = engineer_role;
```

### Snowflake: Create Azure Storage Integration

```sql
-- ============================================
-- 4. CREATE AZURE STORAGE INTEGRATION
-- ============================================
-- Replace:
-- <AZURE_TENANT_ID> - from Azure Service Principal (Directory tenant ID)
-- <STORAGE_ACCOUNT_NAME> - from Azure Storage Account

CREATE STORAGE INTEGRATION azure_ecom_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<AZURE_TENANT_ID>'
  STORAGE_ALLOWED_LOCATIONS = ('azure://<STORAGE_ACCOUNT_NAME>.blob.core.windows.net/raw-data/');

-- Get consent URL
DESC STORAGE INTEGRATION azure_ecom_integration;
-- Copy AZURE_CONSENT_URL value, open in browser, click Accept
```

### Snowflake: Create External Stage

```sql
-- ============================================
-- 5. CREATE EXTERNAL STAGE
-- ============================================
-- Replace <STORAGE_ACCOUNT_NAME>

CREATE STAGE ecom_raw_db.ecom_raw_schema.azure_stage
  STORAGE_INTEGRATION = azure_ecom_integration
  URL = 'azure://<STORAGE_ACCOUNT_NAME>.blob.core.windows.net/raw-data/'
  FILE_FORMAT = (
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
  );

-- Test stage connection
LIST @ecom_raw_db.ecom_raw_schema.azure_stage;
```

### Snowflake: Create Raw Tables

```sql
-- ============================================
-- 6. CREATE RAW TABLES
-- ============================================

USE SCHEMA ecom_raw_db.ecom_raw_schema;

CREATE TABLE raw_orders (
  order_id VARCHAR,
  customer_id VARCHAR,
  product_id VARCHAR,
  order_date VARCHAR,
  quantity VARCHAR,
  unit_price VARCHAR,
  order_status VARCHAR,
  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE raw_customers (
  customer_id VARCHAR,
  customer_name VARCHAR,
  email VARCHAR,
  phone VARCHAR,
  city VARCHAR,
  country VARCHAR,
  registration_date VARCHAR,
  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE raw_products (
  product_id VARCHAR,
  product_name VARCHAR,
  category VARCHAR,
  brand VARCHAR,
  cost VARCHAR,
  price VARCHAR,
  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Snowflake: Create Snowpipes

```sql
-- ============================================
-- 7. CREATE SNOWPIPES
-- ============================================

CREATE PIPE pipe_orders
  AUTO_INGEST = FALSE
  AS
  COPY INTO ecom_raw_db.ecom_raw_schema.raw_orders 
    (order_id, customer_id, product_id, order_date, quantity, unit_price, order_status)
  FROM @ecom_raw_db.ecom_raw_schema.azure_stage/orders/
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE PIPE pipe_customers
  AUTO_INGEST = FALSE
  AS
  COPY INTO ecom_raw_db.ecom_raw_schema.raw_customers 
    (customer_id, customer_name, email, phone, city, country, registration_date)
  FROM @ecom_raw_db.ecom_raw_schema.azure_stage/customers/
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE PIPE pipe_products
  AUTO_INGEST = FALSE
  AS
  COPY INTO ecom_raw_db.ecom_raw_schema.raw_products 
    (product_id, product_name, category, brand, cost, price)
  FROM @ecom_raw_db.ecom_raw_schema.azure_stage/products/
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Verify pipes created
SHOW PIPES;
```

### Snowflake: Trigger Snowpipe

```sql
-- ============================================
-- TRIGGER SNOWPIPES (Manual Refresh)
-- ============================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ecom_loading_wh;

-- Trigger pipes to load data from Azure Blob
ALTER PIPE ecom_raw_db.ecom_raw_schema.pipe_orders REFRESH;
ALTER PIPE ecom_raw_db.ecom_raw_schema.pipe_customers REFRESH;
ALTER PIPE ecom_raw_db.ecom_raw_schema.pipe_products REFRESH;

-- Wait 30-60 seconds, then check load status
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'ecom_raw_db.ecom_raw_schema.raw_orders',
  START_TIME => DATEADD(HOURS, -1, CURRENT_TIMESTAMP())
));

-- Expected: STATUS = 'LOADED'
```

### Snowflake: Verification Queries

```sql
-- ============================================
-- VERIFICATION QUERIES
-- ============================================

USE SCHEMA ecom_raw_db.ecom_raw_schema;

-- Check row counts
SELECT 'raw_orders' AS table_name, COUNT(*) AS row_count FROM raw_orders
UNION ALL
SELECT 'raw_customers', COUNT(*) FROM raw_customers
UNION ALL
SELECT 'raw_products', COUNT(*) FROM raw_products;
-- Expected: 1500, 300, 50

-- Check for duplicate order_ids
SELECT order_id, COUNT(*) AS occurrences
FROM raw_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 10;
-- Expected: ~110 duplicates

-- Check for NULL customer_id
SELECT COUNT(*) AS null_customer_ids
FROM raw_orders
WHERE customer_id IS NULL;
-- Expected: ~55 nulls

-- Check for future dates
SELECT COUNT(*) AS future_orders
FROM raw_orders
WHERE TRY_TO_TIMESTAMP(order_date) > CURRENT_TIMESTAMP();
-- Expected: ~45 future dates
```

---

## 11. Appendix B: Python Data Generator

```python
# data_generator/generate_ecom_data.py

import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

fake = Faker()
Faker.seed(42)
random.seed(42)

# Azure connection
account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
container_name = os.getenv('AZURE_CONTAINER_NAME')

blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=account_key
)

def generate_products(n=50):
    """Generate product catalog with intentional nulls"""
    categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Sports', 'Toys', None]
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE', None]
    
    products = []
    for i in range(1, n+1):
        cost = round(random.uniform(5, 200), 2)
        markup = random.uniform(1.2, 2.5)
        price = round(cost * markup, 2)
        
        products.append({
            'product_id': f'P{i:04d}',
            'product_name': fake.catch_phrase(),
            'category': random.choice(categories),
            'brand': random.choice(brands),
            'cost': cost if random.random() > 0.05 else None,
            'price': price
        })
    
    return pd.DataFrame(products)

def generate_customers(n=300):
    """Generate customer data with intentional duplicates and nulls"""
    customers = []
    emails_used = set()
    
    for i in range(1, n+1):
        email = fake.email()
        
        # Create 5% duplicates by reusing emails
        if random.random() < 0.05 and len(emails_used) > 10:
            email = random.choice(list(emails_used))
        else:
            emails_used.add(email)
        
        customers.append({
            'customer_id': f'C{i:05d}',
            'customer_name': fake.name() if random.random() > 0.02 else None,
            'email': email,
            'phone': fake.phone_number() if random.random() > 0.08 else None,
            'city': fake.city(),
            'country': fake.country(),
            'registration_date': fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
        })
    
    return pd.DataFrame(customers)

def generate_orders(customers_df, products_df, n=1500):
    """Generate orders with intentional quality issues"""
    orders = []
    customer_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()
    
    statuses = ['pending', 'shipped', 'delivered', 'cancelled']
    
    for i in range(1, n+1):
        order_date = fake.date_time_between(start_date='-6M', end_date='now')
        
        # Inject 3% future dates
        if random.random() < 0.03:
            order_date = datetime.now() + timedelta(days=random.randint(1, 30))
        
        order_id = f'O{i:06d}'
        
        # Inject 7% duplicate order_ids
        if random.random() < 0.07 and i > 100:
            order_id = f'O{random.randint(1, i-1):06d}'
        
        # Inject 4% null customer_id
        customer_id = random.choice(customer_ids) if random.random() > 0.04 else None
        
        # Inject 2% null product_id
        product_id = random.choice(product_ids) if random.random() > 0.02 else None
        
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 500), 2)
        
        orders.append({
            'order_id': order_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'quantity': quantity,
            'unit_price': unit_price,
            'order_status': random.choice(statuses)
        })
    
    return pd.DataFrame(orders)

def upload_to_azure(df, folder, filename):
    """Upload CSV to Azure Blob Storage"""
    csv_data = df.to_csv(index=False)
    blob_path = f"{folder}/{filename}"
    
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_path
    )
    
    blob_client.upload_blob(csv_data, overwrite=True)
    print(f"✅ Uploaded {blob_path} ({len(df)} rows)")

def main():
    print("🔄 Generating e-commerce data...")
    
    products_df = generate_products(50)
    customers_df = generate_customers(300)
    orders_df = generate_orders(customers_df, products_df, 1500)
    
    print(f"\n📊 Data Summary:")
    print(f"  Products: {len(products_df)} rows")
    print(f"  Customers: {len(customers_df)} rows")
    print(f"  Orders: {len(orders_df)} rows")
    
    print(f"\n⚠️  Intentional Data Quality Issues:")
    print(f"  Products with NULL category: {products_df['category'].isna().sum()}")
    print(f"  Products with NULL brand: {products_df['brand'].isna().sum()}")
    print(f"  Products with NULL cost: {products_df['cost'].isna().sum()}")
    print(f"  Customers with NULL name: {customers_df['customer_name'].isna().sum()}")
    print(f"  Customers with NULL phone: {customers_df['phone'].isna().sum()}")
    print(f"  Duplicate customer emails: {customers_df['email'].duplicated().sum()}")
    print(f"  Orders with NULL customer_id: {orders_df['customer_id'].isna().sum()}")
    print(f"  Orders with NULL product_id: {orders_df['product_id'].isna().sum()}")
    print(f"  Duplicate order_ids: {orders_df['order_id'].duplicated().sum()}")
    
    future_orders = pd.to_datetime(orders_df['order_date']) > datetime.now()
    print(f"  Orders with future dates: {future_orders.sum()}")
    
    print(f"\n☁️  Uploading to Azure Blob Storage...")
    upload_to_azure(products_df, 'products', 'products_20260322.csv')
    upload_to_azure(customers_df, 'customers', 'customers_20260322.csv')
    upload_to_azure(orders_df, 'orders', 'orders_20260322.csv')
    
    print(f"\n✨ Data generation complete!")

if __name__ == "__main__":
    main()
```

---

## 12. Appendix C: dbt Code

### dbt: sources.yml

```yaml
# models/staging/sources.yml

version: 2

sources:
  - name: ecom_raw
    description: "Raw e-commerce data loaded from Azure Blob via Snowpipe"
    database: ecom_raw_db
    schema: ecom_raw_schema
    
    tables:
      - name: raw_orders
        description: "Unvalidated order transactions - contains duplicates and nulls"
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
        loaded_at_field: _loaded_at

      - name: raw_customers
        description: "Customer master data - has duplicate emails and null names"
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
        loaded_at_field: _loaded_at

      - name: raw_products
        description: "Product catalog - has null categories and brands"
        freshness:
          warn_after: {count: 168, period: hour}
          error_after: {count: 336, period: hour}
        loaded_at_field: _loaded_at
```

### dbt: stg_orders.sql

```sql
-- models/staging/stg_orders.sql

with source as (
    select * from {{ source('ecom_raw', 'raw_orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        product_id,
        try_to_timestamp(order_date) as order_date,
        try_to_number(quantity) as quantity,
        try_to_number(unit_price, 10, 2) as unit_price,
        lower(trim(order_status)) as order_status,
        _loaded_at
    from source
    where 
        order_id is not null
        and customer_id is not null
        and product_id is not null
        and quantity is not null
        and unit_price is not null
        and try_to_timestamp(order_date) <= current_timestamp()
),

deduped as (
    select
        *,
        row_number() over (
            partition by order_id 
            order by _loaded_at desc
        ) as row_num
    from cleaned
),

final as (
    select
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        unit_price,
        round(quantity * unit_price, 2) as total_amount,
        order_status
    from deduped
    where row_num = 1
)

select * from final
```

### dbt: stg_customers.sql

```sql
-- models/staging/stg_customers.sql

with source as (
    select * from {{ source('ecom_raw', 'raw_customers') }}
),

cleaned as (
    select
        customer_id,
        coalesce(nullif(trim(customer_name), ''), 'Unknown') as customer_name,
        lower(trim(email)) as email,
        coalesce(nullif(trim(phone), ''), 'N/A') as phone,
        trim(city) as city,
        trim(country) as country,
        try_to_date(registration_date) as registration_date,
        _loaded_at
    from source
    where customer_id is not null
),

deduped as (
    select
        *,
        row_number() over (
            partition by customer_id 
            order by _loaded_at desc
        ) as row_num_id
    from cleaned
),

final as (
    select
        customer_id,
        customer_name,
        email,
        phone,
        city,
        country,
        registration_date
    from deduped
    where row_num_id = 1
)

select * from final
```

### dbt: stg_products.sql

```sql
-- models/staging/stg_products.sql

with source as (
    select * from {{ source('ecom_raw', 'raw_products') }}
),

cleaned as (
    select
        product_id,
        trim(product_name) as product_name,
        coalesce(nullif(trim(category), ''), 'Uncategorized') as category,
        coalesce(nullif(trim(brand), ''), 'Generic') as brand,
        try_to_number(cost, 10, 2) as cost,
        try_to_number(price, 10, 2) as price,
        _loaded_at
    from source
    where 
        product_id is not null
        and product_name is not null
        and price is not null
),

deduped as (
    select
        *,
        row_number() over (
            partition by product_id 
            order by _loaded_at desc
        ) as row_num
    from cleaned
),

final as (
    select
        product_id,
        product_name,
        category,
        brand,
        cost,
        price,
        case 
            when cost is not null and cost > 0 
            then round(((price - cost) / cost) * 100, 2)
            else null
        end as margin_percent
    from deduped
    where row_num = 1
)

select * from final
```

### dbt: staging/schema.yml

```yaml
# models/staging/schema.yml

version: 2

models:
  - name: stg_orders
    description: "Cleaned and deduped orders - removed duplicates, nulls, and future dates"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      - name: product_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_products')
              field: product_id
      - name: order_date
        tests:
          - not_null
      - name: quantity
        tests:
          - not_null
      - name: unit_price
        tests:
          - not_null
      - name: total_amount
        tests:
          - not_null
      - name: order_status
        tests:
          - not_null
          - accepted_values:
              values: ['pending', 'shipped', 'delivered', 'cancelled']

  - name: stg_customers
    description: "Cleaned customer master - deduplicated by customer_id, nulls replaced"
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: customer_name
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: registration_date
        tests:
          - not_null

  - name: stg_products
    description: "Cleaned product catalog - nulls replaced with defaults"
    columns:
      - name: product_id
        tests:
          - unique
          - not_null
      - name: product_name
        tests:
          - not_null
      - name: category
        tests:
          - not_null
      - name: brand
        tests:
          - not_null
      - name: price
        tests:
          - not_null
```

### dbt: dim_customers.sql

```sql
-- models/marts/dim_customers.sql

{{ config(
    materialized='table',
    schema='ecom_marts'
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_orders as (
    select
        customer_id,
        count(distinct order_id) as total_orders,
        sum(total_amount) as lifetime_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(distinct case when order_status = 'delivered' then order_id end) as delivered_orders,
        count(distinct case when order_status = 'cancelled' then order_id end) as cancelled_orders
    from {{ ref('stg_orders') }}
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.phone,
        c.city,
        c.country,
        c.registration_date,
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.lifetime_value, 0) as lifetime_value,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.delivered_orders, 0) as delivered_orders,
        coalesce(co.cancelled_orders, 0) as cancelled_orders,
        case
            when coalesce(co.lifetime_value, 0) >= 1000 then 'VIP'
            when coalesce(co.lifetime_value, 0) >= 500 then 'Gold'
            when coalesce(co.lifetime_value, 0) >= 100 then 'Silver'
            else 'Bronze'
        end as customer_tier,
        case
            when co.last_order_date >= dateadd(day, -30, current_date) then 'Active'
            when co.last_order_date >= dateadd(day, -90, current_date) then 'At Risk'
            when co.last_order_date is not null then 'Churned'
            else 'New'
        end as customer_status,
        current_timestamp() as _updated_at
    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
)

select * from final
```

### dbt: dim_products.sql

```sql
-- models/marts/dim_products.sql

{{ config(
    materialized='table',
    schema='ecom_marts'
) }}

with products as (
    select * from {{ ref('stg_products') }}
),

product_performance as (
    select
        product_id,
        count(distinct order_id) as total_orders,
        sum(quantity) as total_quantity_sold,
        sum(total_amount) as total_revenue,
        avg(unit_price) as avg_selling_price,
        min(order_date) as first_sold_date,
        max(order_date) as last_sold_date
    from {{ ref('stg_orders') }}
    group by product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.cost,
        p.price,
        p.margin_percent,
        coalesce(pp.total_orders, 0) as total_orders,
        coalesce(pp.total_quantity_sold, 0) as total_quantity_sold,
        coalesce(pp.total_revenue, 0) as total_revenue,
        pp.avg_selling_price,
        pp.first_sold_date,
        pp.last_sold_date,
        case
            when coalesce(pp.total_revenue, 0) = 0 then 'Never Sold'
            when pp.last_sold_date >= dateadd(day, -30, current_date) then 'Active'
            when pp.last_sold_date >= dateadd(day, -90, current_date) then 'Slow Moving'
            else 'Discontinued'
        end as product_status,
        current_timestamp() as _updated_at
    from products p
    left join product_performance pp on p.product_id = pp.product_id
)

select * from final
```

### dbt: fct_orders.sql

```sql
-- models/marts/fct_orders.sql

{{ config(
    materialized='table',
    schema='ecom_marts'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select 
        customer_id,
        customer_tier,
        customer_status,
        country
    from {{ ref('dim_customers') }}
),

products as (
    select
        product_id,
        category,
        brand,
        cost,
        margin_percent
    from {{ ref('dim_products') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.product_id,
        o.order_date,
        date(o.order_date) as order_date_key,
        o.quantity,
        o.unit_price,
        o.total_amount,
        o.order_status,
        
        -- Customer attributes
        c.customer_tier,
        c.customer_status,
        c.country as customer_country,
        
        -- Product attributes
        p.category as product_category,
        p.brand as product_brand,
        p.cost as product_cost,
        
        -- Calculated metrics
        round(o.quantity * p.cost, 2) as total_cost,
        round(o.total_amount - (o.quantity * coalesce(p.cost, 0)), 2) as gross_profit,
        p.margin_percent,
        
        -- Time dimensions
        extract(year from o.order_date) as order_year,
        extract(month from o.order_date) as order_month,
        extract(day from o.order_date) as order_day,
        extract(quarter from o.order_date) as order_quarter,
        dayname(o.order_date) as order_day_of_week,
        
        -- Flags
        case when o.order_status = 'delivered' then 1 else 0 end as is_delivered,
        case when o.order_status = 'cancelled' then 1 else 0 end as is_cancelled,
        
        current_timestamp() as _updated_at
    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join products p on o.product_id = p.product_id
)

select * from final
```

### dbt: marts/schema.yml

```yaml
# models/marts/schema.yml

version: 2

models:
  - name: fct_orders
    description: "Order fact table with customer and product context"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      - name: product_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_products')
              field: product_id
      - name: total_amount
        tests:
          - not_null
      - name: gross_profit
        tests:
          - not_null

  - name: dim_customers
    description: "Customer dimension with lifetime metrics"
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - not_null
      - name: customer_tier
        tests:
          - accepted_values:
              values: ['VIP', 'Gold', 'Silver', 'Bronze']
      - name: customer_status
        tests:
          - accepted_values:
              values: ['Active', 'At Risk', 'Churned', 'New']
      - name: lifetime_value
        tests:
          - not_null

  - name: dim_products
    description: "Product dimension with sales performance"
    columns:
      - name: product_id
        tests:
          - unique
          - not_null
      - name: product_name
        tests:
          - not_null
      - name: price
        tests:
          - not_null
      - name: product_status
        tests:
          - accepted_values:
              values: ['Never Sold', 'Active', 'Slow Moving', 'Discontinued']
```

### dbt: assert_revenue_matches.sql

```sql
-- tests/assert_revenue_matches.sql

with staging_revenue as (
    select 
        sum(total_amount) as staging_total,
        count(*) as staging_count
    from {{ ref('stg_orders') }}
    where order_status != 'cancelled'
),

marts_revenue as (
    select 
        sum(total_amount) as marts_total,
        count(*) as marts_count
    from {{ ref('fct_orders') }}
    where order_status != 'cancelled'
)

select
    s.staging_total,
    m.marts_total,
    abs(s.staging_total - m.marts_total) as revenue_difference,
    s.staging_count,
    m.marts_count,
    abs(s.staging_count - m.marts_count) as count_difference
from staging_revenue s
cross join marts_revenue m
where abs(s.staging_total - m.marts_total) > 0.01
   or s.staging_count != m.marts_count
```

### dbt: macros/generate_schema_name.sql

```sql
-- macros/generate_schema_name.sql

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}
```

---

## 13. Appendix D: Dagster Code

### Dagster: __init__.py

```python
# dagster_ecommerce/__init__.py

from dagster import Definitions
from .assets.dbt_assets import dbt_analytics_assets
from .assets.azure_assets import azure_blob_asset
from .assets.snowpipe_assets import snowpipe_refresh
from .sensors.file_sensor import azure_file_sensor
from .sensors.alert_sensor import alert_on_pipeline_failure
from .schedules.daily_schedule import daily_analytics_schedule
from .jobs.daily_job import all_assets_job, dbt_only_job
from .resources.dbt_resource import dbt_resource
from .resources.snowflake_resource import snowflake_resource
from .resources.azure_resource import azure_resource

defs = Definitions(
    assets=[azure_blob_asset, snowpipe_refresh, dbt_analytics_assets],
    sensors=[azure_file_sensor, alert_on_pipeline_failure],
    schedules=[daily_analytics_schedule],
    jobs=[all_assets_job, dbt_only_job],
    resources={
        "dbt": dbt_resource,
        "snowflake": snowflake_resource,
        "azure": azure_resource,
    },
)
```

### Dagster: Resources

**dbt_resource.py:**

```python
# dagster_ecommerce/resources/dbt_resource.py

import os
from pathlib import Path
from dagster_dbt import DbtCliResource

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_ecommerce"

dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
)
```

**snowflake_resource.py:**

```python
# dagster_ecommerce/resources/snowflake_resource.py

import os
from dagster import ConfigurableResource
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

class SnowflakeResource(ConfigurableResource):
    account: str = os.getenv('SNOWFLAKE_ACCOUNT')
    user: str = os.getenv('SNOWFLAKE_USER')
    password: str = os.getenv('SNOWFLAKE_PASSWORD')
    warehouse: str = os.getenv('SNOWFLAKE_WAREHOUSE')
    database: str = os.getenv('SNOWFLAKE_DATABASE')
    role: str = os.getenv('SNOWFLAKE_ROLE')
    
    def get_connection(self):
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role
        )
    
    def execute_query(self, query: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            conn.close()

snowflake_resource = SnowflakeResource()
```

**azure_resource.py:**

```python
# dagster_ecommerce/resources/azure_resource.py

import os
from dagster import ConfigurableResource
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

class AzureBlobResource(ConfigurableResource):
    account_name: str = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
    account_key: str = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
    container_name: str = os.getenv('AZURE_CONTAINER_NAME')
    
    def get_client(self):
        return BlobServiceClient(
            account_url=f"https://{self.account_name}.blob.core.windows.net",
            credential=self.account_key
        )
    
    def list_blobs(self, folder_prefix: str = ""):
        client = self.get_client()
        container_client = client.get_container_client(self.container_name)
        return list(container_client.list_blobs(name_starts_with=folder_prefix))

azure_resource = AzureBlobResource()
```

### Dagster: Assets

**azure_assets.py:**

```python
# dagster_ecommerce/assets/azure_assets.py

from dagster import asset, AssetExecutionContext
from ..resources.azure_resource import AzureBlobResource

@asset(
    description="Monitor Azure Blob Storage for new CSV files",
    group_name="ingestion"
)
def azure_blob_asset(context: AssetExecutionContext, azure: AzureBlobResource):
    """Check for files in Azure Blob Storage"""
    
    folders = ['orders', 'customers', 'products']
    file_summary = {}
    
    for folder in folders:
        blobs = azure.list_blobs(folder_prefix=folder)
        file_summary[folder] = len(blobs)
        
        context.log.info(f"Found {len(blobs)} files in {folder}/")
        for blob in blobs:
            context.log.info(f"  - {blob.name} ({blob.size} bytes)")
    
    return file_summary
```

**snowpipe_assets.py:**

```python
# dagster_ecommerce/assets/snowpipe_assets.py

from dagster import asset, AssetExecutionContext
from ..resources.snowflake_resource import SnowflakeResource

@asset(
    description="Trigger Snowpipe to load new files from Azure Blob",
    group_name="ingestion",
    deps=["azure_blob_asset"]
)
def snowpipe_refresh(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """Manually refresh Snowpipes to load new data"""
    
    pipes = [
        'ecom_raw_db.ecom_raw_schema.pipe_orders',
        'ecom_raw_db.ecom_raw_schema.pipe_customers',
        'ecom_raw_db.ecom_raw_schema.pipe_products'
    ]
    
    results = []
    for pipe in pipes:
        query = f"ALTER PIPE {pipe} REFRESH"
        context.log.info(f"Refreshing {pipe}")
        
        try:
            snowflake.execute_query(query)
            results.append({"pipe": pipe, "status": "success"})
            context.log.info(f"✅ {pipe} refreshed")
        except Exception as e:
            context.log.error(f"❌ {pipe} failed: {str(e)}")
            results.append({"pipe": pipe, "status": "failed", "error": str(e)})
    
    return results
```

**dbt_assets.py:**

```python
# dagster_ecommerce/assets/dbt_assets.py

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_ecommerce"

@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json"
)
def dbt_analytics_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    dbt models for e-commerce analytics
    Includes staging layer (data quality) and marts layer (business logic)
    """
    yield from dbt.cli(["build"], context=context).stream()
```

### Dagster: Sensors

**file_sensor.py:**

```python
# dagster_ecommerce/sensors/file_sensor.py

from dagster import sensor, RunRequest, SensorEvaluationContext, SkipReason
from ..resources.azure_resource import AzureBlobResource
import os
from dotenv import load_dotenv

load_dotenv()

@sensor(
    name="azure_file_sensor",
    minimum_interval_seconds=300,
    description="Detects new CSV files in Azure Blob and triggers pipeline"
)
def azure_file_sensor(context: SensorEvaluationContext):
    """
    Sensor that watches Azure Blob Storage for new files
    Triggers Snowpipe refresh and dbt run when new files detected
    """
    
    azure = AzureBlobResource()
    
    orders_files = azure.list_blobs(folder_prefix='orders')
    customers_files = azure.list_blobs(folder_prefix='customers')
    products_files = azure.list_blobs(folder_prefix='products')
    
    total_files = len(orders_files) + len(customers_files) + len(products_files)
    last_count = int(context.cursor) if context.cursor else 0
    
    if total_files > last_count:
        context.log.info(f"New files detected! Previous: {last_count}, Current: {total_files}")
        context.update_cursor(str(total_files))
        
        return RunRequest(
            run_key=f"file_count_{total_files}",
            run_config={},
            tags={
                "new_files": str(total_files - last_count),
                "total_files": str(total_files)
            }
        )
    
    return SkipReason(f"No new files. Current count: {total_files}")
```

**alert_sensor.py:**

```python
# dagster_ecommerce/sensors/alert_sensor.py

from dagster import run_failure_sensor, RunStatusSensorContext

@run_failure_sensor(
    name="alert_on_failure",
    description="Send alert when any run fails"
)
def alert_on_pipeline_failure(context: RunStatusSensorContext):
    """
    Triggered whenever a Dagster run fails
    Sends alert with failure details
    """
    
    alert_message = f"""
    🚨 *PIPELINE FAILURE ALERT* 🚨
    
    *Run ID:* {context.dagster_run.run_id}
    *Job:* {context.dagster_run.job_name}
    *Status:* {context.dagster_run.status}
    *Failed At:* {context.dagster_run.end_time}
    
    *Error:*
    Check Dagster UI for full logs.
    """
    
    context.log.error(alert_message)
```

### Dagster: Schedules

**daily_schedule.py:**

```python
# dagster_ecommerce/schedules/daily_schedule.py

from dagster import schedule, RunRequest, ScheduleEvaluationContext

@schedule(
    name="daily_analytics_refresh",
    cron_schedule="0 2 * * *",
    job_name="daily_analytics_job",
    description="Runs full analytics pipeline daily at 2 AM"
)
def daily_analytics_schedule(context: ScheduleEvaluationContext):
    """
    Daily schedule that:
    1. Checks Azure Blob for files
    2. Refreshes Snowpipe
    3. Runs all dbt models
    """
    return RunRequest(
        run_key=f"daily_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        tags={
            "schedule": "daily",
            "execution_time": context.scheduled_execution_time.isoformat()
        }
    )
```

### Dagster: Jobs

**daily_job.py:**

```python
# dagster_ecommerce/jobs/daily_job.py

from dagster import define_asset_job, AssetSelection

all_assets_job = define_asset_job(
    name="daily_analytics_job",
    selection=AssetSelection.all(),
    description="Run all analytics assets daily"
)

dbt_only_job = define_asset_job(
    name="dbt_refresh_job",
    selection=AssetSelection.groups("default"),
    description="Refresh dbt models only"
)
```

---

## End of Documentation

**Project Status:** Parts 1-5 Complete ✅  
**Next Steps:** Implement Parts 6-8 (Security, Testing, SCD Type 2)  
**Version:** 1.0  
**Last Updated:** March 22, 2026

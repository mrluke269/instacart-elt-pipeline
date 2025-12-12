# Instacart Operations Analytics Pipeline
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/pandas-150458?style=flat&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)


An automated data pipeline that processes grocery orders daily, transforms raw data into analytics-ready tables, and delivers operational insights through interactive dashboards.

---
## [<img src="https://img.icons8.com/?size=512&id=19318&format=png" width="15">] Full Pipeline Walkthrough

[<img src="https://img.icons8.com/?size=512&id=19318&format=png" width="120">](https://youtu.be/jkZCXvYTi28)

## Business Objective

This pipeline processes Instacart grocery order data to support operational decisions around inventory, staffing, and promotions. It demonstrates how production data systems handle real-world requirements:

- **Incremental processing** — extract only new data each day, not full reloads
- **Idempotent loads** — safe to rerun or backfill without creating duplicates
- **Layered transformations** — separate data cleaning from business logic
- **Automated orchestration** — scheduled runs with error handling and retries

> **Note:** The source dataset (3.4M orders) was scaled to 3K orders across 60 days to create realistic incremental processing scenarios for development and testing.

---

## Architecture

![Architecture Diagram](./images/architechture.png)

The pipeline runs inside Docker containers with volume mounts connecting the Windows host filesystem to the containerized environment.

**Data Flow:**
- Python scripts extract data from CSV files and load to Snowflake's RAW layer
- dbt transforms data through staging (cleaning) and marts (dimensional models)
- Power BI queries the marts directly for visualization

**Control Plane:**
- Airflow orchestrates the entire workflow — triggering extraction, loading, transformation, and testing in sequence
- Scheduled daily with retry logic and error handling

---

## Tech Stack

![Tech Stack Overview](./images/tech_stack.png)

---

## Key Features

![Pipeline Key Features](./images/key_features.png)

---

## Data Model

![Data Model](./images/data_model.png)

The fact table (`fct_order_products`) captures each line item in an order, linking to dimension tables for product and order context. This structure supports efficient queries for basket analysis, reorder patterns, and temporal trends.

---

## Design Decisions

![Architectural Decisions & Trade-offs](./images/decisions.png)

---

## Project Structure

![Project Structure](./images/structure.png)

---

## Dashboard

The pipeline delivers operational insights through a two-page Power BI dashboard.

### Operations Overview

![Operations Report](./reports/operation_report.png)

- **KPIs**: 350 orders processed, 9.9 average basket size, 11.8 days between orders
- **Peak Ordering Time**: Heatmap reveals busiest hours × days (Sunday 1 PM, Friday noon)
- **Temporal Patterns**: Sunday leads with 70 orders; peak hours are 10 AM – 2 PM

### Product Segmentation

![Product Segmentation](./reports/segmentation_report.png)

- **Customer Buying Habits**: Scatter plot segments products by volume (X) vs reorder rate (Y)
- **Strategic Segments**:
  - 🟢 Green: High volume + high reorder — priority stock (Banana, Organic Strawberries)
  - 🔵 Blue: High volume + lower reorder — popular but not habit-forming
  - 🔴 Red: Lower volume — long tail products
- **Key Insight**: Organic products dominate the top sellers

---

## Data Source

[Instacart Market Basket Analysis](https://www.kaggle.com/c/instacart-market-basket-analysis) — Kaggle dataset containing 3.4M grocery orders from 200K+ users.

---
<div align="center">


## 📧 Contact & Links

**GitHub:** [github.com/mrluke269]  
**Email:** [luke.trmai@gmail.com]
### 

**Luke M**

</div>

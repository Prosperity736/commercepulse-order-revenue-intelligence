# CommercePulse — Order & Revenue Intelligence

> An end-to-end commerce data engineering and business intelligence project transforming raw transactional data into scalable analytical datasets and executive decision intelligence.

##  Project Status

**In Progress**

The data engineering foundation and Executive Supply Chain Overview are currently complete. Additional Power BI analytical pages are being developed.

---

##  Overview

CommercePulse is an end-to-end analytics platform built around a real-world e-commerce dataset.

The project demonstrates the complete journey from raw transactional data to business-ready analytics:

**Raw Data → Python → PySpark → Silver Layer → Gold Layer → Power BI**

The goal is to create a reliable analytical foundation that enables executives and business stakeholders to understand revenue performance, order activity, customer behavior, product performance, fulfillment, seller performance, payments, and geographic trends.

---

##  Business Problem

Raw commerce transactions contain valuable information about orders, customers, products, sellers, payments, freight, and delivery performance.

However, raw transactional data is not immediately suitable for executive decision-making.

CommercePulse addresses this by transforming the raw data into validated, business-ready analytical datasets and presenting the resulting metrics through an interactive Power BI reporting layer.

---

##  Data Architecture

```text
                    RAW COMMERCE DATA
                           │
                           ▼
                    Python Ingestion
                           │
                           ▼
                    PySpark Processing
                           │
                           ▼
                    ┌───────────────┐
                    │ Silver Layer  │
                    │               │
                    │ Cleaned &     │
                    │ transformed   │
                    │ datasets      │
                    └───────┬───────┘
                            │
                            ▼
                    ┌───────────────┐
                    │  Gold Layer   │
                    │               │
                    │ Dimensions    │
                    │ Fact Tables   │
                    │ Analytics     │
                    └───────┬───────┘
                            │
                            ▼
                    Executive Analytics
                            │
                            ▼
                       Power BI
                            │
                            ▼
              CommercePulse Intelligence

Technology Stack
Layer	Technology
Programming	Python
Distributed Processing	PySpark
Processing Engine	Apache Spark
Local Hadoop Support	Hadoop Windows Utilities
Data Transformation	PySpark
Analytics Layer	Python / PySpark
BI & Visualization	Power BI
BI Calculations	DAX
Testing	Pytest
Development	VS Code
Version Control	Git / GitHub


commercepulse-order-revenue-intelligence/
│
├── config/
│
├── notebooks/
│
├── src/
│   │
│   ├── ingestion/
│   │   ├── ingest_olist.py
│   │   ├── spark_session.py
│   │   └── utils.py
│   │
│   ├── transformation/
│   │   ├── build_silver.py
│   │   ├── silver_customers.py
│   │   ├── silver_geolocation.py
│   │   ├── silver_order_items.py
│   │   ├── silver_order_payments.py
│   │   ├── silver_order_reviews.py
│   │   ├── silver_orders.py
│   │   ├── silver_product_category_translation.py
│   │   ├── silver_products.py
│   │   └── silver_sellers.py
│   │
│   ├── validation/
│   │   └── validate_bronze.py
│   │
│   └── gold/
│       │
│       ├── gold_fact_orders.py
│       ├── dim_customers.py
│       ├── dim_date.py
│       ├── dim_products.py
│       ├── dim_sellers.py
│       │
│       └── analytics/
│           ├── customer_summary.py
│           ├── delivery_summary.py
│           ├── executive_summary.py
│           ├── geography_summary.py
│           ├── payment_summary.py
│           ├── product_summary.py
│           ├── reconcile_gold.py
│           ├── sales_summary.py
│           └── seller_summary.py
│
├── tests/
│
├── requirements.txt
├── README.md
└── .gitignore



Data Pipeline
1. Ingestion

The pipeline begins with raw Olist e-commerce datasets.

Python and PySpark are used to establish the processing environment and ingest the source datasets.

2. Silver Transformation

The raw datasets are transformed into cleaned and structured Silver datasets.

Transformations cover areas including:

Orders
Customers
Products
Sellers
Order items
Payments
Reviews
Geolocation
Product category translation
3. Gold Analytics

The Gold layer creates business-ready analytical structures.

This includes:

Customer dimension
Product dimension
Seller dimension
Date dimension
Order fact table
Executive analytics
Sales analytics
Customer analytics
Product analytics
Delivery analytics
Seller analytics
Payment analytics
Geographic analytics
4. Validation

Analytical outputs are validated before being consumed by the reporting layer.

The project includes automated tests for ingestion, transformation, Spark sessions, and key datasets.

5. Business Intelligence

Power BI consumes the analytical outputs and presents the information through executive-level dashboards and interactive reporting.

 Executive Supply Chain Overview

The first completed Power BI reporting layer is the Executive Supply Chain Overview.

It provides visibility into:

Revenue
Orders
Average Order Value
Fulfillment
On-Time Delivery
Open Order Backlog
Freight % of Sales
Average Order Cycle Time
Product Category Performance
Monthly Performance
Executive Summary

The executive summary is dynamically driven by the selected reporting month.

 Current Executive Analytics

The Gold Executive Summary currently produces metrics including:

Metric	Value
Total Orders	98,666
Total Customers	95,420
Total Products	32,951
Total Sellers	3,095
Total Sales Value	$15.84M
Total Freight	$2.25M
Average Order Value	$160.58
Average Review Score	4.04
On-Time Delivery Rate	93.23%

These represent the current analytical dataset and are not production business KPIs from a live company.

 Engineering Approach

The project follows a layered data architecture to separate raw data processing from business-facing analytics.

This provides:

Clear separation of responsibilities
Reusable transformations
Easier validation
More maintainable analytical logic
A foundation for scaling the project
A clean boundary between engineering and BI

The Gold layer is designed around business questions rather than simply reproducing the raw source tables.

 Testing & Validation

The repository includes tests covering:

Bronze dataset validation
Customer data
Silver customers
Silver geolocation
Silver order items
Silver order payments
Silver order reviews
Silver orders
Silver products
Silver sellers
Spark session configuration

The Gold layer also includes reconciliation logic to validate analytical outputs.

 Data

The project uses the Olist Brazilian E-Commerce dataset.

Raw source datasets are intentionally excluded from this GitHub repository through .gitignore.

This keeps the repository focused on:

Pipeline code
Transformations
Analytics
Tests
Configuration
Documentation

 Current Development
Completed
 Python project structure
 Spark environment
 Olist data ingestion
 Silver transformation layer
 Gold dimensions
 Gold fact table
 Gold analytics datasets
 Executive analytics
 Data validation
 Automated tests
 Power BI Executive Supply Chain Overview
 Dynamic executive summary
 Executive KPI calculations
 Git/GitHub repository
In Progress
 Sales & Revenue Intelligence
 Customer Intelligence
 Product & Category Performance
 Fulfillment & Delivery Analytics
 Seller Performance
 Payment Intelligence
 Geographic Intelligence

 Future Improvements

Planned improvements include:

Dockerizing the pipeline
Introducing SQL-based analytical processing
Pipeline orchestration with Airflow
Automated data quality checks
Cloud deployment
Pipeline scheduling
Additional Power BI analytical pages
Production-oriented monitoring

These are future enhancements and are not represented as completed functionality.

 Role

Data Engineer & BI Analyst

Responsibilities across the project include:

Data ingestion
Data transformation
PySpark development
Data modeling
Analytical engineering
Data validation
KPI development
DAX development
Power BI dashboard development
Git/GitHub version control

 Project Status

CommercePulse is currently in active development.

The data engineering foundation and Executive Supply Chain Overview establish the first complete analytical slice of the platform. Additional intelligence pages will be added as the project progresses.

 Data Source

Olist Brazilian E-Commerce Public Dataset.

This project is intended for educational, portfolio, and demonstration purposes.
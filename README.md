# 🏗️ Data Lakehouse Project: Bronze → Silver → Gold Pipeline

## 🚀 Overview
This project implements a robust **multi-layered ETL pipeline** using the **Bronze, Silver, and Gold architecture pattern**. The goal is to build a clean, query-optimized data warehouse foundation for reporting and analytics.

---

## 🧱 Layered Architecture

![image](https://github.com/user-attachments/assets/0798a30a-b2c2-4938-b2a2-00796e66fadb)












### 🔸 Bronze Layer – Raw Ingested Data
- Stores **raw source data** ingested from operational systems
- No transformations applied; preserves original structure
- Acts as the **single source of truth** for historical data

### 🔹 Silver Layer – Cleaned and Transformed Data
- Cleans, deduplicates, and standardizes raw data
- Applies **business rules, validation, and enrichment**
- Intermediate, analysis-ready layer
- Stored in structured format for ease of querying

### 🥇 Gold Layer – Business-Ready Data
- Aggregated, summarized, and denormalized data
- Aligned with **business KPIs and reporting needs**
- Supports dashboards, advanced analytics, and ML models

---

## 📦 Key Features
- SQL-based transformations with row-level validation
- Runtime tracking using timestamps and logging
- Modular, maintainable procedures for each layer
- Try-Catch blocks for error handling and traceability
- Domain logic applied to customer, product, and sales datasets

---

## 🛠️ Technologies Used
- Microsoft SQL Server
- T-SQL Stored Procedures
- SQL Window Functions
- Git for version control

---

## 📈 Sample Transformations
- `TRIM`, `UPPER`, `CASE` for value standardization
- `ROW_NUMBER()` to deduplicate records
- `LEAD()` to derive end-dates in product pricing
- Business logic to fix invalid sales and pricing data

---

## 💡 Use Cases
- Data Warehousing
- Business Intelligence and Reporting
- Building Features for Machine Learning Models
- End-to-End Data Engineering Workflows

---

## 👤 Author
[Your Name]  
[LinkedIn](https://www.linkedin.com/in/yourprofile) | [Portfolio](https://yourportfolio.com)

---

## 📄 License
MIT License (or your preferred license)


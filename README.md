
# Lakehouse Engine: Automated Medallion Data Pipeline

An end-to-end, production-grade Data Lakehouse pipeline designed using the **Medallion Architecture (Bronze $\to$ Silver $\to$ Gold)**. Built with **PySpark**, **Delta Lake**, and orchestrated through containerized **Apache Airflow**.

---

## 🏗️ Architecture Overview

```text
Landing (Raw JSON Batches)
       │
       ▼
 [Bronze Tier]  ──────► Quarantine Mart (_corrupt_record isolation)
       │ (Append-Only Delta Table)
       ▼
 [Silver Tier]  (Struct Flattening, Salted SHA-256 PII Masking, CDC Upsert via Delta MERGE INTO)
       │ (Partitioned by Order Status)
       ▼
  [Gold Tier]   (Curated Business Marts: Daily Revenue Metrics & Category Performance)
```

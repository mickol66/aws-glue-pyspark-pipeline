# Serverless Data Lake Pipeline: AWS Glue PySpark ETL & Amazon Athena Data Lake

## 📝 Project Overview
This repository contains a production-grade, end-to-end **Serverless Data Lake Pipeline** built on AWS. 

The pipeline automates the ingestion, structural cleansing, type casting, and schema optimization of raw transactional store records (Chinook schema). Built entirely via **Infrastructure as Code (IaC)** using AWS CloudFormation, the infrastructure is deployed through an automated continuous integration stream (**GitHub Actions** with AWS Access authentication). The architecture transforms incoming non-relational event stream files into query-ready analytics partitions with high cost-efficiency (FinOps).

---

## 🛠️ Architecture & Core Components

- **Infrastructure as Code (IaC):** **AWS CloudFormation** mapping unified declarative specifications for storage layers, serverless compute configurations, and security policies.
- **CI/CD Automation Pipeline:** **GitHub Actions Workflows** driving secure code validation and programmatic deployment paths into AWS.
- **Storage Layer (Data Lake Architecture):**
  - **Landing Zone (S3):** Immutable object store hosting raw, non-relational transactional event logs formatted as newline-delimited JSON.
  - **Analytics Zone (S3):** Columnar, high-performance object store holding optimized datasets compressed via **Snappy Parquet** formats.
- **Serverless Compute Layer:** **AWS Glue 4.0 ETL Engine** driving a containerized distributed **Apache Spark (PySpark)** environment optimized with specific memory guardrails.
- **Data Catalog & Analytics Engine:** **AWS Glue Data Catalog** metadata manager integrated with **Amazon Athena** for serverless ad-hoc SQL querying.

---

## 🚀 Key Technical Implementation Steps

### Phase 1: Declarative Infrastructure Deployment (IaC)
1. Formulated standard CloudFormation resources establishing strict S3 block-public-access parameters for enterprise-grade asset protection.
2. Engineered fine-grained IAM Execution Roles limiting operational privileges exclusively to the target landing and transformation buckets (Principle of Least Privilege).
3. Configured the AWS Glue Job resource embedding dynamic parameterization, timeout triggers, and strict worker sizing (`WorkerType: G.1X`, `NumberOfWorkers: 2`) to eliminate runtime waste.

### Phase 2: Distributed Data Transformation Engine (PySpark)
1. Developed an active PySpark script using **AWS Glue DynamicFrames** to seamlessly ingest variable structural JSON event files.
2. Executed data quality cleansing, including standardizing geographic metadata to uppercase formats and casting fields to strict analytical datatypes (`Integer`, `Double`).
3. Embedded tracking metadata columns (`processed_at` ingest timestamps) to establish auditable data lineage.
4. Structured the physical sink operations using **Data Lake Partitioning** (`partitionKeys=["country"]`) combined with Snappy Parquet serialization to maximize analytical query performance.

### Phase 3: Serverless Analytical Presentation (Schema-on-Read)
1. Designed an analytical presentation layer within Amazon Athena utilizing decoupled external table metadata mappings.
2. Executed manual partition registration structures (`ALTER TABLE ADD PARTITION`) to cleanly register active operational data branches.
3. Conducted ad-hoc SQL execution pipelines validating structural performance and data integrity directly over S3 objects.

---

## 📊 Performance, Optimization & Troubleshooting

- **Storage Compression Efficiency:** Leveraging column-oriented Snappy Parquet achieved drastic storage footprint reductions compared to the verbose raw JSON streams.
- **Query Execution & Cost Control:** Utilizing physical partitioning per country restricted the data volume scanned during analytics, converting directly into sub-penny analytical runtime expenses.
- **Real-World Problem Solving:** Remedied underlying compute telemetric start collisions (`LAUNCH ERROR`) inherent within isolated containerized AWS environments by engineering explicit operational parameter overrides (`--enable-container-telemetry-collection = false`).

---

## 🔍 Validation Queries & Analytical Results

The integrity of the pipeline was validated by executing ad-hoc queries over the partition layers using Amazon Athena:

```sql
-- Analytical Validation Statement
SELECT customerid, firstname, lastname, country, processed_at, total 
FROM default.processed_raw_portfolio_dump;
```

### Expected Output Structure:

| customerid | firstname | lastname  | country | processed_at               | total  |
|------------|-----------|-----------|---------|----------------------------|--------|
| 4501       | Mikael    | Johansson | Sweden  | 2026-10-07 15:10:24.123    | 50.0   |
| 4502       | David     | Nilsson   | Sweden  | 2026-10-07 15:10:24.125    | 25.5   |
| 4503       | Fredrik   | Karlsson  | Sweden  | 2026-10-07 15:10:24.129    | 120.0  |

---

## 💡 Key Takeaways & Enterprise Business Value

1. **Production-Grade Automation:** Proved the capacity to provision and deploy end-to-end data lake architectures flawlessly via automated Git triggers with zero cloud console dependencies.
2. **Columnar Big Data Optimization:** Applied industry-standard Apache Spark engineering patterns, converting distributed raw logs into partition-optimized files.
3. **Resilient Cloud Engineering:** Showcased the technical analytical skills required to decode deep, opaque service exceptions, translating system log issues into operational infrastructure solutions.

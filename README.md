# E-Commerce Data Warehouse Pipeline (EcomDW)

## Overview

This project demonstrates an **end-to-end data engineering pipeline** built using **SQL Server** and **SSIS**.

The system:

* Generates **synthetic sales data** using **Python**
* Ingests the data through an **automated SSIS ETL pipeline**
* Loads the data into a **dimensional data warehouse (star schema)**
* Exposes the data for **analytics and reporting using SSRS**

The goal of this project is to simulate a **production-style data warehouse pipeline**, demonstrating data ingestion, transformation, dimensional modeling, and analytical reporting.


## Key Engineering Concepts Demonstrated

✔ Dimensional Modeling:
Designed a star schema including DimDate, DimProduct, DimStore, and FactSales.

✔ Incremental Fact Loading:
Implemented UPDATE + INSERT logic to support idempotent pipeline execution.

✔ Surrogate Key Resolution:
Natural keys resolved through dimension lookups during fact loading.

✔ Data Quality Validation:
Custom validation stored procedure ensures pipeline integrity before loading.

✔ Lineage Tracking:
SourceFile and LoadDttm columns enable full auditability.



<div align="center">

![Visual](EcomDW_Project/Images/Visual_Architecture_img.png)
</div>


<div align="center">

## 🛠 Tech StackTech Stack

![Python](https://img.shields.io/badge/Python-Data%20Generation-yellow)
![SQL Server](https://img.shields.io/badge/SQL%20Server-Database-red)
![SSIS](https://img.shields.io/badge/SSIS-ETL-blue)
![SSRS](https://img.shields.io/badge/SSRS-Reporting-green)
![T-SQL](https://img.shields.io/badge/T--SQL-Data%20Transformation-orange)
![Visual Studio](https://img.shields.io/badge/Visual%20Studio-Development-purple)
![GitHub](https://img.shields.io/badge/GitHub-Version%20Control-black)
</div>


### 1️⃣ Warehouse Schema
The warehouse follows a star schema design optimized for analytical workloads.
![Warehouse Schema](EcomDW_Project/Images/EcomDw_Diagram.png)



### 2️⃣ SSIS Control Flow
The ETL pipeline is orchestrated using SQL Server Integration Services (SSIS).
![SSIS Control Flow](EcomDW_Project/Images/SSIS_Control_Flow.png)

# Key Pipline Stages Include:
Pipeline initialization

✔ Staging loads

✔ Dimension upserts

✔ Fact table loading

✔ Validation framework

✔ Execution logging

---

### 3️⃣ Data Flow
CSV files are processed through an SSIS data flow which performs transformations before loading staging tables.
![Data Flow](EcomDW_Project/Images/DataFlow_File_Ingestion.png)

<div align="center">

# Transformations performed:

Data type conversions        •       Error handling paths     •          Staging table insertion
</div>




  
### 4️⃣ Staging Layer
Raw source data is first loaded into the staging table:

stg.SalesRaw

The staging layer serves several purposes:

Preserve raw source data

Enable validation and cleansing

Support reprocessing if needed

---


### 5️⃣ Deduplication Logic
To enforce the correct grain of the dataset, a staging view removes duplicate records.

Grain Enforced:

SalesDate
ProdectId
StoreId

Implementation uses a window function:

ROW_NUMBER() OVER (
PARTITION BY SaleDate, ProductId, StoreId
ORDER BY LoadDttm DESC
)

Only the most recent record per grain is retained.
---


### 6️⃣ Dimension Upserts
Dimension tables are maintained using an upsert pattern:

Update existing dimension records if attributes change

Insert new dimension records when the natural key does not exist

This ensures the warehouse dimensions stay synchronized with the incoming source data.

---

### 7️⃣ Fact Table Loading
Fact records are loaded after dimension tables are updated.

The fact table grain is defined as:
DateKey
ProductKey
StoreKey

Surrogate keys are resolved by joining staging data to dimension tables.

Duplicate facts are prevented using NOT EXISTS checks.

### 8️⃣ Pipeline Observability
Each pipeline execution generates a unique RunId.

This identifier allows all ETL steps to be associated with a single execution.

Execution metadata is stored in:

etl.RunLog

Example tracked information:

Pipeline step name

Row counts

Execution status

Timestamps


### 9️⃣ Data Validation Framework

A validation stored procedure acts as a data quality gate before pipeline completion.
![Data Validation](EcomDW_Project/Images/usp_ValidationLoad_image.png)

## Validation checks include:

Row count comparisons across pipeline stages

Duplicate fact grain detection

Missing dimension key checks

Domain rule validation

Price anomaly detection

If critical validation rules fail, the pipeline stops execution.

---

### 🔟 SSRS Sales Overview Report
The warehouse powers an SSRS report that provides an overview of sales performance.
![Data Validation](EcomDW_Project/Images/rpt_MonthTotals.png)

<div align="center">
  
Users can filter data by:

Category

Region

Brand

Store

Date range

</div>


### 1️⃣1️⃣ Drill-Through Detail Report
![Drill- Through Report](EcomDW_Project/Images/Drill_Through_Report.png)


<div align="center">


---


# Future Enhancements:

Potential improvements include:

Incremental loading strategies

Slowly changing dimension support

Automated anomaly detection

Orchestration with modern tools such as Airflow


</div>







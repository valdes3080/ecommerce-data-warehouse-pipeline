# E-Commerce Data Warehouse Pipeline (EcomDW)

## Overview

This project demonstrates an **end-to-end data engineering pipeline** built using **SQL Server** and **SSIS**.

The system:

* Generates **synthetic sales data** using **Python**
* Ingests the data through an **automated SSIS ETL pipeline**
* Loads the data into a **dimensional data warehouse (star schema)**
* Exposes the data for **analytics and reporting using SSRS**

The goal of this project is to simulate a **production-style data warehouse pipeline**, demonstrating data ingestion, transformation, dimensional modeling, and analytical reporting.

<div align="center">

## 🛠 Tech Stack

![Python](https://img.shields.io/badge/Python-Data%20Generation-teal)
![SQL Server](https://img.shields.io/badge/SQL%20Server-Database-teal)
![SSIS](https://img.shields.io/badge/SSIS-ETL-teal)
![SSRS](https://img.shields.io/badge/SSRS-Reporting-teal)
![T-SQL](https://img.shields.io/badge/T--SQL-Data%20Transformation-teal)
![Visual Studio](https://img.shields.io/badge/Visual%20Studio-Development-teal)
![GitHub](https://img.shields.io/badge/GitHub-Version%20Control-teal)
</div>


## Data Engineering Practices

This project was designed to simulate production-style data engineering workflows.

Key practices demonstrated include:

- **Dimensional modeling** using a star schema (FactSales with supporting dimensions)
- **Automated ETL pipelines** using SSIS
- **Synthetic data generation** using Python to simulate real-world datasets
- **Incremental data loading** patterns to support repeatable ETL runs
- **Surrogate key resolution** between staging data and dimension tables
- **Data quality validation** to ensure referential integrity
- **Separation of staging and warehouse layers**



<div align="center">

![Visual](EcomDW_Project/Images/Visual_Architecture_img.png)
</div>



## Pipeline Walkthrough

The animation below demonstrates how data moves through the ETL pipeline from ingestion to analytics.

<div align="center">

![EcomDW Pipeline](EcomDW_Project/Images/ecomdw_pipeline_animation_v2.gif)

</div>

---

<div align="center">

![Model](EcomDW_Project/Images/ETL_DataProccessing_Overviewgif.gif)


</div>

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







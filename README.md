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

![ETL_PROCESS](EcomDW_Project/Images/ETL_process_at_a_glance.gif)


</div>

---




###  Data Validation Framework

A validation stored procedure acts as a data quality gate before pipeline completion.
![Data Validation](EcomDW_Project/Images/usp_ValidationLoad_image.png)

<div align="center">

## Validation checks include:

:hash: Row count comparisons across pipeline stages

:repeat_one: Duplicate fact grain detection

:mag_right: Missing dimension key checks

:lock: Domain rule validation

:moneybag: Price anomaly detection

:warning: If critical validation rules fail, the pipeline stops execution.

</div>


---

###  SSRS Sales Overview Report
The warehouse powers an SSRS report that provides an overview of sales performance.
![Data Validation](EcomDW_Project/Images/MonthlyTotals_rpt.gif)

---

<div align="center">

  
Users can filter data by:

:clipboard:  Category

:earth_americas:  Region

:sparkles:  Brand

:convenience_store:  Store

:calendar:  Date range

</div>
---



<div align="center">


# Future Enhancements:

Potential improvements include:

Incremental loading strategies

Slowly changing dimension support

Automated anomaly detection

Orchestration with modern tools such as Airflow


</div>


## Author

Tatiana Valdes  
Aspiring Data Engineer | SQL Developer | Data Analyst  

GitHub: https://github.com/valdes3080






# Data Engineering with AWS Certification from Udacity

This repository contains the code and projects completed as part of the **Data Engineering with AWS Nanodegree** from Udacity. The projects demonstrate my ability to build scalable data pipelines, data warehouses, and data lakehouse solutions using AWS technologies.

## Projects Included

### 1. Data Warehouse
   - **Description:** Developed a data warehouse for Sparkify, a music streaming startup. The goal is to analyze user activity and song data stored in S3 to provide insights into user behavior and inform product decisions.
   - **Tech Stack:** 
     - AWS S3 (data storage)
     - AWS Redshift (data warehouse)
     - Python and SQL (ETL pipeline)
   - **Key Concepts:**
     - Data modeling (Star schema)
     - ETL processes for JSON data
     - SQL queries for analysis

### 2. Data Pipelines with Airflow
   - **Description:** Built a data pipeline using Apache Airflow to extract data from an S3 bucket, load it into a Redshift data warehouse, and perform data quality checks. The pipeline ensures data accuracy and allows for easy monitoring of data lineage.
   - **Tech Stack:**
     - Apache Airflow (pipeline orchestration)
     - AWS S3 (data storage)
     - AWS Redshift (data warehouse)
   - **Key Concepts:**
     - Data pipeline automation
     - Data quality checks
     - Monitoring and logging with Airflow

### 3. STEDI Human Balance Analytics
   - **Description:** Extracted sensor data from the STEDI Step Trainer project and implemented a Data Lakehouse solution on AWS. The data is stored in a format suitable for Data Scientists and Analysts to visualize or use in training machine learning models.
   - **Tech Stack:**
     - AWS S3 (data lake)
     - AWS Glue (ETL and data catalog)
     - AWS Redshift (data warehouse)
   - **Key Concepts:**
     - Data lakehouse architecture
     - ETL pipelines for semi-structured data
     - Preparing data for machine learning models

## Installation and Setup
1. Clone this repository:
   ```bash
   git clone https://github.com/neidynm/udacity.git
   ```
2. Follow the instructions within each project folder for the specific setup, dependencies, and execution steps.

## Technologies Used
- **Cloud:** AWS (S3, Redshift, Lambda, Glue)
- **Orchestration:** Apache Airflow
- **Programming Languages:** Python, SQL
- **Data Formats:** JSON, Parquet

## License
This repository is licensed under the [MIT License](LICENSE).

## Contact
If you have any questions or want to connect, feel free to reach me via [LinkedIn](https://www.linkedin.com/in/your-linkedin-profile).

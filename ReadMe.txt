Instagram Data Warehouse - README

Project Overview

The Instagram Data Warehouse is designed to store and analyze user engagement data from an Instagram-like platform. The warehouse uses Databricks, Delta Lake, and Azure Blob Storage to efficiently manage interactions such as likes, comments, follows, and uploads.

Repository Structure

📂 Instagram_Data_Warehouse
│-- 📂 data                # Raw CSV files
│-- 📂 scripts             # ETL scripts and SQL queries
│-- 📂 reports             # Final report and documentation
│-- README.md              # Project documentation (this file)

Data Sources

Data is extracted from multiple sources, including:

User activity logs (likes, comments, follows)

Photo metadata (uploads, tags, interactions)

External sources (future expansion for ad tracking, engagement metrics)

ETL Process

Extract: Load raw data from Azure Blob Storage.

Transform: Clean, normalize, and structure data using Spark.

Load: Store data in Delta tables and optimize performance.

Database Schema

The database follows a star schema, consisting of:

Fact Table: fact_interactions (stores user interactions)

Dimension Tables: dim_users, dim_photos, dim_tags, dim_interaction_types

Junction Table: photo_tags (manages many-to-many relationships)

Setup Instructions

1. Prerequisites

Databricks runtime environment

Azure Blob Storage configured

Python (for ETL scripting)

SQL (for querying tables)

2. Steps to Run the Project

Clone the repository:

git clone https://github.com/your-repo/Instagram_Data_Warehouse.git

Upload CSV files to Azure Blob Storage.

Open Databricks and run the ETL_pipeline.py script.

Execute SQL queries for data validation.

Use Tableau/Power BI for visualization.

Key Features

Incremental Data Processing: Updates data efficiently without duplication.

SCD Type 2 Implementation: Maintains historical changes in users and photos.

Optimized Queries: Uses indexing and partitioning for performance.

Future Improvements

Implement real-time streaming with Apache Kafka.

Develop predictive engagement models using machine learning.

Expand support for advertisement analytics and revenue tracking.

Contact Information

For any issues or questions, please reach out via:

Email: shreevikasbj@gmail.com

GitHub Issues: https://github.com/Shreevikas-BJ/DataWarehousing
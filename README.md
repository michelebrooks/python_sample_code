# Data Engineering Python Sample Code

This is a collection of sample python scripts that I maintained and refactored when I took over the roles and responsibilities of our data engineer. There are three python scripts that together to form a part of the ETL portion of the data pipeline for a mobile music streaming application. 

All of the scripts were managed and executed in AWS usingn the following workflow:
- EC2 servers ran for SFTP data pipeline where bash scripts pulled these raw json files and deposited them into an S3 Data Lake. 
- Python ETL scripts were stored in S3 and executed using Lambda and Glue Data Catalog to access raw data S3 Data Lake. 
- Python ETL scripts running in Lambda spun up on-demand EMR clusters to execute ETL SQL files and deposit back into S3 to form a Data Lakehouse. 
- Tableau reports and SQL queries accessed Data Lakehouse via Athena to gather insights to report back to the business.

*NB: sensitive information redacted. Please pardon gaps in the code*

#### music-app-partition-user-activity-logs.py ####
Pulls raw JSON log files from data lake, adds date partitions and compressing them into GZIP format for efficient tables for cost-optimised and performant Athena queries

#### music-app-cms-pipeline.py ####
Pulls raw album and track data from a CMS server, deposits it into the S3 data lake and transforms it into a usable content library

#### music-app-etl-emr.py ####
Runs an ETL job using on-demand EMR clusters to transform logs and content library into a "data lakehouse" in Athena that is used for SQL and Tableau reporting.
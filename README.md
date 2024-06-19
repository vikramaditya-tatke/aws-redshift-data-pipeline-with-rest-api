# AWS Redshift Data Pipeline with Rest API

This repository contains a robust and efficient Python-based data pipeline designed to extract large volumes of search result data from IBM QRadar SIEM and load it into Amazon Redshift Serverless for analysis and reporting.


### Key Features:

Optimized for Big Data: Handles massive QRadar search result sets (30 million+ records) using streaming techniques and parallel processing to minimize memory usage and maximize throughput.
Minimized API Calls: Reduces the number of calls to the QRadar API to avoid rate limiting and improve performance.
Redshift Bulk Loading: Leverages the Redshift COPY command for fast and efficient bulk loading of JSON data.
Error Handling and Retries: Includes comprehensive error handling with retry mechanisms for API requests and database operations to ensure data reliability.
Logging: Provides detailed logging for monitoring, troubleshooting, and auditing purposes.
Production-Ready: Designed with scalability, maintainability, and observability in mind.
Extensible: Easily adaptable to different QRadar search criteria and custom Redshift schemas.


### Technology Stack:

Python: Core programming language.
Requests: Library for making HTTP requests to the QRadar API.
Psycopg2: PostgreSQL adapter for connecting to Redshift.
ThreadPoolExecutor: For parallel processing of data fetching tasks.
Amazon Redshift Serverless: Cloud-based data warehouse for storing and analyzing the ingested data.
Getting Started:


### Orchestration (Optional):
Updates will be made to this README and the repository, to use Dagster or Airflow as the orchestrators.

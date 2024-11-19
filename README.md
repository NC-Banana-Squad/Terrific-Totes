Terrific Totes Data Pipeline
Overview

The Terrific Totes Data Pipeline is a data engineering project that simulates the backend operations of a company selling customized tote bags. This system is designed to extract operational data from a PostgreSQL database, transform it into analytical models, and store it in a data warehouse optimized for business intelligence queries. After the initial extraction of existing data, the extraction of updates is run every 20 min. The relevant updates are reflected in BI dashboard in real time.

The project demonstrates end-to-end ETL (Extract, Transform, Load) processes using AWS and Terraform, and includes:

    Automated data ingestion from a simulated operational database.
    Data transformation to remodel operational data into analytical schemas.
    Data loading into a cloud-based warehouse.

Key Features

    Data Ingestion: Extracts data from a PostgreSQL database and stores raw files in an S3 bucket.
    Data Transformation: Processes raw files into analytics-ready data in Parquet format, stored in another S3 bucket.
    Data Warehouse Loading: Loads transformed data into a relational data warehouse following star schema models.
    Infrastructure as Code: Manages cloud infrastructure using Terraform.
    AWS Lambda Integration: Utilizes serverless Lambda functions to execute the ETL steps with AWS triggers.
    Comprehensive Testing: Ensures functionality with 100% test coverage using mocking and patching techniques.

Architecture

    Operational Database: PostgreSQL database simulating Terrific Totes' business operations.
    Data Lake: S3 buckets storing raw and processed data.
    Data Warehouse: Remodelled datasets organized into "Sales," "Purchases," and "Payments" schemas for analytical querying.
    Terraform: Automates infrastructure creation and management.
    Python: Core logic for ETL processes written in Python, with dependencies managed through virtual environments.

Setup Instructions
Prerequisites

    Python 3.9.16 or compatible.
    Terraform installed locally.
    AWS account credentials with S3, Lambda, and IAM permissions.

Deployment
GitHub Actions (Cloud)

    Fork the repository and configure repository secrets (DATABASE_CREDENTIALS, AWS_ACCESS_KEY, etc.).
    Update bucket names in configuration files.
    Trigger the deployment workflow using GitHub Actions.

Local Deployment

    Clone the repository and set up a Python virtual environment.
    Configure AWS credentials and create an S3 bucket for Terraform state files.
    Initialize and apply the Terraform configuration.
    Reconfigure deploy.yml to work with your branch if necessary
    The pipeline will be deployed automatically after "push" and will run scheduled extraction of updates every 20 min

Folder Structure

    src/: Contains Python scripts for ETL stages (<ingestion.py>, <transformation.py>, <population.py>).
    terraform/: Infrastructure as code files, including Lambda configurations and S3 buckets.
    tests/: Test cases for ETL scripts.

Testing and Validation

    Mocking and patching ensure accurate testing of AWS services.
    Run tests with pytest to validate functionality across all ETL stages.

Feel free to refine this with specific file or function details from the repository as needed. Let me know if you’d like help expanding any section!
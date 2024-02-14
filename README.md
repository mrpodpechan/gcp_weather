# gcp_weather
Overview
This project implements a data engineering solution on Google Cloud Platform (GCP), leveraging Cloud Functions, Cloud Storage, Cloud Run, and Cloud SQL with a PostgreSQL instance. The objective is to showcase a scalable and efficient data pipeline for processing, storing, and serving data.

Architecture
The architecture of this project comprises the following components:

Cloud Functions: These serverless functions are triggered by http.post workflow calls. They handle tasks such as data transformation, validation, or loading.

Cloud Storage: Serves as storage for raw data ingestion and intermediate data storage. 

Cloud Run: This platform is used for executing Cloud Function workflows. 

Cloud SQL: A managed relational database service provided by GCP. PostgreSQL is used as the database engine for storing structured data.

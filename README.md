## Creating an ELT Data Pipeline for Chicago Taxi Trips

## Welcome to my Chicago Taxitrips ETL Pipeline

This project focuses on building a comprehensive Extract, Load, Transform (ELT) data pipeline to collect information from the Chicago Data Portal's Taxi Trips dataset using API. The extracted data is then loaded into a PostgreSQL database, transferred to a Google Cloud Storage (GCS) bucket, and finally moved to BigQuery.

## Problem Description
The project aims to address the need for a scalable and efficient data pipeline to process and analyze taxi trips data from the City of Chicago. By automating the extraction, loading, and transformation processes, the pipeline provides a structured and optimized dataset in Google BigQuery for advanced analytics and reporting.The project is developed in the cloud, leveraging Google Cloud Platform (GCP) services such as Google Cloud Storage and BigQuery.

## Data Architecture

A robust data architecture is crucial for ensuring data quality, integrity, and accessibility. Here's the architecture for my Chicago Data Pipeline:

![Data Architecture](https://github.com/salmah52/chicagotaxitrips/assets/44398948/cd0f92d6-4158-408b-92c7-e9487eda8333")


1. **Data Source:**
   - Chicago Data Portal API (Taxi Trips dataset)

2. **ETL (Extract, Transform, Load) Pipeline:**
   - Apache Airflow for orchestration and automation
   - Custom operators for extracting data from the API and loading it into PostgreSQL and Google Cloud Storage (GGCS)
   - Google Cloud Storage (GCS) for intermediate storage of raw data

3. **Data Warehouse:**
   - Google BigQuery for data warehousing
   - Create a dedicated dataset in BigQuery (e.g., raw_data_table)
   - Use tables in BigQuery to store raw and transformed data

4. **Data Transformation:**
   - dbt (Data Build Tool) for data modeling, transformation, and analytics

5. **Data Reporting and Visualisation:**
   - PowerBI for Reporting and Visualisation

## Dataset Description

- **Name:** Taxi Trips
- **Category:** Transportation
- **Data Source:** City of Chicago
- **Description:** Taxi trips reported to the City of Chicago in its role as a regulatory agency. To protect privacy but allow for aggregate analyses, the Taxi ID is consistent for any given taxi medallion number but does not show the number, Census Tracts are suppressed in some cases, and times are rounded to the nearest 15 minutes.

## Columns in the Dataset - Data Dictionary

- **Rows:** 211M
- **Columns:** 23

# Project Stages

## STEP1- Data Extraction and Loading (Extract and Load Pipeline)

### Code Explanation

This code defines a custom Airflow Operator (`TaxitripsToPostgresOperator`) responsible for extracting data from the Chicago taxi trips API and loading it into a PostgreSQL database. Let's break down the key components:

- **Initialization:**
  - The constructor (`__init__` method) initializes the parameters, including the API details, PostgreSQL connection ID and table name.

- **Execution Method (`execute`):**
  - Constructs the API URL based on the provided endpoint.
  - Sets up parameters for the API request, including limit and order.
  - Sends a GET request to the API with proper headers and parameters.
  - Checks for errors in the API response.
  - If the response is successful (status code 200), converts the JSON data to a Pandas DataFrame and drops unnecessary columns (`pickup_centroid_location` and `dropoff_centroid_location`).
  - Logs information about the data extraction.
  - Uses SQLAlchemy to send the processed DataFrame to the PostgreSQL database.

### Airflow DAG Definition

This code defines an Airflow Directed Acyclic Graph (DAG) to orchestrate the data extraction and loading process. Key details include:

- **DAG Configuration (`dag`):**
  - Defines the DAG with a unique identifier ('taxitrips_extraction_dag0009'), a description, and a schedule interval (every day in this case).

- **Tasks:**
  - `start_task`: A dummy task marking the start of the DAG.
  - `extract_and_load_task`: An instance of the `TaxitripsToPostgresOperator` representing the data extraction and loading task. Configured with specific parameters.
  - `end_task`: Another dummy task marking the end of the DAG.

- **Task Dependencies (`start_task >> extract_and_load_task >> end_task`):**
  - Specifies the order in which tasks should be executed. In this case, the DAG starts with `start_task`, followed by `extract_and_load_task`, and finally `end_task`.


## STEP 2- Data Transformation and Integration (ELT Pipeline):

Transfer the data from a PostgreSQL database to Google Cloud Storage (GCS) as a data lake and subsequently loading it into Google BigQuery for advanced analytics.

## STEP 3- Transformation Layer and Analytics Modeling (dbt Implementation)

This is the overview of the dbt (data build tool) project for the Chicago Taxi Trips dataset. The project follows a dimensional modeling approach to transform raw data into a structured analytics-ready format. The primary objectives include creating dimension and fact tables, generating diverse metrics, and implementing tests for data accuracy and reliability.


## Data Modelling

<img width="630" alt="image" src="https://github.com/salmah52/project01-zoomcamp/assets/44398948/f8884f24-3aad-476b-86ce-7ac02e4a15dd">

### Project Structure

The dbt project is organized into the following directories:

- **data/:** Contains raw data files.
- **models/:** Contains dbt models for staging, dimensions, facts, and metrics.
- **macros/:** Stores reusable SQL code snippets (macros).
- **analysis/:** Houses SQL queries for ad-hoc analysis.

### Staging Data

**stg_taxitrips Model:**
The staging model stg_taxitrips is responsible for ingesting and staging raw data from the data/ directory. It performs basic transformations such as data type casting and cleaning. The raw data includes information about taxi trips in Chicago.

### Dimensional Modeling

**Dimensions:**

1. **dim_date Model:**
   Extracts temporal attributes from the trip_start_timestamp and trip_end_timestamp. Attributes include start and end times, hour, day, month, year, weekday/weekend classification, quarter, and season.

2. **dim_drop_off Model:**
   Extracts information related to drop-off locations, including community area, latitude, longitude, location name, and census tract.

3. **dim_payment Model:**
   Extracts payment-related information, including payment types.

4. **dim_pick_up Model:**
   Extracts information related to pick-up locations, including community area, latitude, longitude, location name, and census tract.

5. **dim_COMPANY:**
   Extracts information related to the company.

**Facts:**

- **fact_taxitrips Model:**
  Combines data from the staging model and dimensions to create a fact table. It includes information such as trip details, fare, tips, trip duration, and more. The model adheres to the star schema for effective data modeling.

### Metrics Modeling

These Metrics models provide valuable insights into revenue trends, average revenue per trip, customer retention rates, and customer segmentation based on usage patterns.

**Metrics Models:**

1. **Revenue Metrics Model (revenue_metrics):**
   - Total Revenue Over Time: Calculates total revenue (trip_total) over time.
   - Revenue by Month, Quarter, and Year: Aggregates revenue at different time granularities.
   - Average Revenue Per Trip: Calculates the average revenue per trip.
   - Top Revenue-Generating Taxis or Companies: Identifies top revenue-generating entities.

2. **Usage Metrics Model (usage_metrics):**
   - Total Number of Trips: Calculates the total number of trips.
   - Number of Trips by Hour, Day, Month, and Year: Aggregates trip counts at different time granularities.
   - Average Trip Duration: Calculates the average duration of trips.
   - Busiest Hours, Days, and Months: Identifies the busiest periods.

3. **Customer Metrics Model (customer_metrics):**
   - Number of Unique Customers: Calculates the count of unique customers.
   - Customer Retention Rate: Measures customer retention over time.
   - Average Trips Per Customer: Calculates the average trips taken by each customer.
   - Customer Segments Based on Usage Patterns: Segments customers based on their usage patterns.

4. **Geospatial Metrics Model (geospatial_metrics):**
   - Popular Pickup and Drop-off Locations: Identifies the most popular locations for pickups and drop-offs.
   - Distance Covered by Taxis: Calculates the total distance covered by taxis.
  
  
# Lineage Graph

![Lineage Graph](https://github.com/salmah52/project01-zoomcamp/assets/44398948/7aba35fd-9b3c-4ebe-aabf-424d47330f95")

<img width="622" alt="image" src="https://github.com/salmah52/project01-zoomcamp/assets/44398948/3e791b86-0039-4b1f-a502-81e4d065a310">


![Data Architecture](https://github.com/salmah52/chicagotaxitrips/assets/44398948/cd0f92d6-4158-408b-92c7-e9487eda8333")




### Tests

**Test Models:**

1. **Data Quality Tests (data_quality_tests):**
   - Ensures that critical fields in the stg_taxitrips model do not contain NULL values.
   - Validates that timestamps in the dim_date model fall within expected date ranges.
   - Verifies the integrity of foreign keys in fact and dimension tables.

2. **Metric Accuracy Tests (metric_accuracy_tests):**
   - Validates the accuracy of calculated metrics in revenue, usage, customer, and geospatial models.


## Dashboard
A PowerBI dashboard provides insights into the taxi trips data:
Tiles:
- Revenue trends
- Trip counts by hour, day, month
- Customer retention rates
- Popular pickup and drop-off locations

## Why This Approach

- **Custom Operator:**
  - The custom operator encapsulates the logic for data extraction and loading, promoting reusability and readability.

- **Dynamic API Configuration:**
  - The operator is configurable, allowing users to specify API details, order, and other parameters during instantiation.

- **Airflow DAG:**
  - The DAG orchestrates the entire process, defining the sequence of tasks to be executed.

This design promotes modularity, making it easy to extend the pipeline for future stages and modifications. The DAG provides a visual representation of the workflow, making it comprehensible and adaptable to changing requirements.


## Conclusion

The completion of the ELT data pipeline for Chicago Taxi Trips marks a significant milestone in enhancing data processing efficiency and unlocking advanced analytics capabilities. This project seamlessly integrates data from the Chicago Data Portal's Taxi Trips API into a structured format, ready for insightful analytics. Here are key takeaways:

# Achievements
- Data Workflow Automation: The adoption of Apache Airflow orchestrates the end-to-end data workflow, automating the extraction, loading, and transformation processes. This not only improves efficiency but also ensures data consistency and reliability.

- Flexible Data Transformation: Leveraging Pandas DataFrame in the custom operator allows for dynamic and flexible data manipulation before loading into PostgreSQL. This flexibility is essential for handling diverse datasets and accommodating future modifications.

- Robust Testing Framework: The inclusion of data quality tests and metric accuracy tests using dbt ensures the integrity and accuracy of the transformed data. This robust testing framework establishes confidence in the reliability of analytics models and metrics.

- Scalability and Containerization: The Docker-ready design of the project facilitates easy deployment and scalability. This approach aligns with modern containerization practices, ensuring adaptability to changing infrastructure requirements.

In conclusion, this ELT data pipeline lays the groundwork for a robust data analytics ecosystem. It not only addresses current analytical needs but also positions the project for future enhancements and evolving data requirements. The project's modular and scalable design ensures its adaptability to the dynamic landscape of data analytics.
# Challenges Overcome

- Dynamic API Configuration: Implementing a custom operator with dynamic API configuration allows users to specify API details, order, and parameters during instantiation. This ensures the pipeline's adaptability to changes in the data source.

- Comprehensive Analytics Modeling: The dimensional modeling approach using dbt enables the creation of structured analytics-ready tables. This provides a solid foundation for generating diverse metrics and analytics insights.

# Reproducibility
# Running the Pipeline
- Clone the repository: git clone https://github.com/salmah52/chicagotaxitrips.git
- Navigate to the project directory: cd project01-zoomcamp
- Set up Airflow with the provided airflow.cfg configuration.
- Run the following command to start Airflow: airflow webserver -p 8080
- Access Airflow UI in your browser: http://localhost:8080
- Trigger the taxitrips_extraction_dag DAG to start the pipeline.


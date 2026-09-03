# Data Engineering Laboratory - Exercises 1 to 5

## Exercise 1: Data Preprocessing and Feature Engineering

**Aim:**
To collect data from diverse sources, perform data preprocessing, cleaning, feature engineering, and exploratory data analysis (EDA).

**Procedure:**
1. **Data Collection:** Identify and collect data from various diverse sources (e.g., text, video, images, audio, medical data).
2. **Exploratory Data Analysis (EDA):** Perform initial EDA to understand the data's distributions, trends, and characteristics using statistical summaries.
3. **Data Preprocessing & Cleaning:** Clean the data by identifying and handling missing values, and treating noisy or inconsistent data.
4. **Feature Engineering:** Apply feature selection to identify the most relevant variables and perform dimensionality reduction if necessary.
5. **Data Normalization:** Standardize or normalize the data to ensure features are on a consistent scale.
6. **Interpretation:** Visualize the cleaned and preprocessed data to evaluate the effectiveness of the applied transformations.

---

## Exercise 2: Building Core Data Pipeline (ETL)

**Aim:**
To build a core ETL data pipeline that extracts data from diverse sources, applies data transformation techniques, implements data loading strategies, and integrates Change Data Capture (CDC) concepts.

**Procedure:**
1. **Data Extraction:** Implement extraction strategies to pull data from diverse sources such as REST APIs, RDBMS, NoSQL databases, and flat files/cloud storage.
2. **Data Transformation:** Apply transformation techniques to cleanse, standardize, join, and aggregate the extracted datasets.
3. **Data Loading Strategies:** Implement and contrast data loading strategies, specifically distinguishing between a Full Load and an Incremental Load.
4. **Change Data Capture (CDC):** Integrate advanced extraction concepts like CDC to detect new, updated, deleted, or unchanged records for incremental processing.
5. **Performance Evaluation:** Monitor and optimize the overall performance of the ETL pipeline.

---

## Exercise 3: Data Architecture & Schema Design

**Aim:**
To design operational (OLTP) and analytical (OLAP) schemas, build dimensional models (Star/Snowflake Schema), and design data cubes for multidimensional analysis.

**Procedure:**
1. **OLTP/OLAP Component Identification:** Identify and differentiate between OLTP (transactional) and OLAP (analytical) components for a provided business use case.
2. **Relational Schema Design:** Design a normalized relational schema (OLTP) focusing on referential integrity and fast transaction processing.
3. **Dimensional Modeling:** Design a corresponding dimensional data model using a Star or Snowflake Schema tailored for analytical workloads.
4. **Data Cube Design:** Build multidimensional data cubes by combining the central Fact Table with surrounding Dimension Tables.
5. **Analytical Queries:** Execute OLAP operations (Roll-up, Drill-down, Slice, Dice) and analytical queries to analyze and interpret the results.

---

## Exercise 4: Python-based Batch Pipeline

**Aim:**
To implement an end-to-end, basic Python-based batch pipeline (using Pandas or PySpark) to extract data, transform it, and load it into a target database or data warehouse.

**Procedure:**
1. **Data Extraction:** Implement scripts to extract data from API sources or flat files into a processing environment (e.g., Pandas DataFrames).
2. **Data Cleaning and Transformation:** Perform automated data cleaning operations, handle outliers/nulls, and apply the required business transformations.
3. **Data Loading:** Load the transformed data systematically into a target database or data warehouse architecture.
4. **Pipeline Functionality:** Orchestrate the steps into an automated batch process to ensure end-to-end pipeline functionality.
5. **Error Handling and Verification:** Implement error handling mechanisms, log pipeline operations, and verify the idempotency of the results to ensure no data duplication occurs on reruns.

---

## Exercise 5: Designing Resilient and Production-Ready Pipelines

**Aim:**
To design resilient, production-ready data pipelines featuring secure staging areas, data quality checks, idempotency, atomicity, and robust error handling strategies.

**Procedure:**
1. **Staging and Validation:** Create secure staging areas (e.g., writing to temporary directories) and implement rigorous data quality checks (schema validation, null checks, outlier detection).
2. **Idempotency:** Build pipeline operations that can be executed safely multiple times without causing data duplication or unintended side effects.
3. **Atomicity:** Ensure "all-or-nothing" transactions (e.g., atomic file writes via rename operations) to prevent partial data loads in the event of a failure.
4. **Error Handling (Retries):** Implement resilient error handling, such as exponential backoff retries, to recover from transient failures.
5. **Backfilling and Replay:** Design backfilling and replay strategies to allow for historical data fixes and seamless recovery from pipeline crashes, utilizing pipeline state and run history logs.

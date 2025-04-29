# Data Pipeline Project

This project is designed to automate the process of fetching raw data from an API, uploading it to AWS S3, loading it into Snowflake, and transforming the data using dbt (Data Build Tool) to create structured fact and dimension tables. The entire pipeline is orchestrated using Apache Airflow.

## **Overview**

The pipeline consists of the following key steps:
1. Fetching raw data from an API and saving it locally.
2. Uploading the data from the local system to an S3 bucket.
3. Loading the data from the S3 bucket into Snowflake.
4. Transforming the data using dbt to create fact and dimension tables.

## **Technologies Used**
- **Python**: Core scripting language for processing the data.
- **Apache Airflow**: Workflow orchestration.
- **AWS S3**: Cloud storage for raw data.
- **Snowflake**: Data warehouse for storing and querying the data.
- **dbt**: For transforming raw data into structured tables (fact and dimension tables).

## **Project Setup**

### **Step 1: Moving Data from API to Local System**

- **API Fetching**: The raw data is fetched from a specified API and saved locally.
- **Process**: The fetched data is saved as JSON files to the local file system.

### **Step 2: Uploading Local Files to S3**

1. **Creating an S3 Bucket**:
   - Go to the AWS Console and create an S3 bucket (e.g., `gym_raw_data_pipeline`).

2. **Creating IAM User**:
   - Create an IAM user with full access to the S3 bucket and generate the AWS Access Key ID and Secret Key.

3. **Setting up Airflow Connection**:
   - In Apache Airflow, create a new connection for AWS with the Access Key and Secret Key.

4. **Uploading Data**:
   - Using an S3 hook in Airflow, upload the local data to the S3 bucket.
   
   Example Airflow DAG for uploading files:
   ```python
   from airflow.providers.amazon.aws.hooks.s3 import S3Hook

   s3_hook = S3Hook(aws_conn_id='aws_default')
   local_file_path = "/path/to/local/file.json"
   bucket_name = "gym_raw_data_pipeline"
   key_name = "data/file.json"

   s3_hook.load_file(local_file_path, key_name, bucket_name)
   ```
5. **Cleanup**:
   - After successfully uploading, the local files are deleted to free up space.
  
### Step 3: Loading Data From S3 to Snowflake 
1. **Create Snowflake Database**:
  - Log in to Snowflake and create a new database (e.g., gym_db).
2. **Create Snowflake Stage**:
  - Set up a Snowflake stage to reference the S3 bucket.
  - Provide the necessary credentials for the IAM user to access the S3 bucket.
3. **Create Tables in Snowflake**:
  - Define tables to store data, ensuring relationships between tables using foreign keys.
  - Example Snowflake SQL for creating tables:
    ```
    CREATE TABLE user_account_details (
    id INT PRIMARY KEY,
    username STRING,
    email STRING );
    ```
4. **Load Data**:
  - Use Snowflake's COPY INTO command to load data from the S3 stage into the Snowflake tables.
5. **Airflow Task for Snowflake**:
  - Create an Airflow task to execute the Snowflake SQL commands.
  - Example Airflow task for loading data:
    ```
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
    snowflake_task = SnowflakeOperator(
    task_id='load_data_to_snowflake',
    sql="COPY INTO gym_db.user_account_details FROM @gym_stage;",
    snowflake_conn_id="snowflake_default"
    )
    ```
### Step 4: Transforming Data Using dbt
1. **Set up dbt Project**:
  - Initialize a dbt project in the Airflow directory.
  - Configure the connection to the Snowflake warehouse and database for dbt.
2. **Grant Permissions**:
  - Ensure the necessary permissions are granted for dbt to access the source and target databases.
3. **Define dbt Models**:
  - Create dbt models for transforming the raw data into fact and dimension tables.
4. **Run dbt**:
  - Execute dbt transformations to structure the data. Example dbt model for transforming raw user data:
    ```
    -- models/intermediate_user_profile.sql
    SELECT
    id,
    username,
    email,
    signup_date
    FROM {{ source('raw_user_data', 'user_account_details') }}
    ```
5. **Airflow Integration**:
  - Create an Airflow task to run the dbt transformations.
  - Example Airflow task for dbt:
    ```
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
    dbt_task = DbtCloudRunJobOperator(
    task_id="run_dbt_transforms",
    dbt_cloud_conn_id="dbt_cloud_default",
    job_id="your_dbt_job_id"
    )
    ```
## Running the Project

1. **Install Dependencies**:
  - Install the required Dependencies and libraries:
    ```
    pip install apache-airflow
    pip install apache-airflow-providers-amazon
    pip install apache-airflow-providers-snowflake
    pip install dbt
    pip install snowflake-connector-python
    ```
2. **Start Airflow**:
  - Start the Airflow web server and scheduler:
    ```
    airflow webserver -p 8080
    airflow scheduler
    ```
3. **Trigger the DAG**:
  - Manually trigger the DAG from the Airflow UI or configure it to run on a schedule.
  - Verify Data in Snowflake:
  - After the DAG completes successfully, check the Snowflake tables to ensure that data has been loaded and transformed correctly.
    




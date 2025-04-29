import os
import json
import shutil
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

API_BASE_URL = "http://host.docker.internal:8090"
S3_BUCKET_NAME = "gym-raw-datapipeline"

API_ENDPOINTS = {
    "user_personal_details": "/app/services/api/v1/getUserPersonalDetails/date",
    "user_health_details": "/app/services/api/v1/getHealthDetailsByDate/date",
    "user_account_details": "/app/services/api/v1/getUserAccountDetails/date",
    "message_center": "/app/services/api/v1/getMessagesByDate/date",
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="daily_data_fetch_and_load",
    default_args=default_args,
    description="Fetch gym data, upload to S3, and transform/load to Snowflake",
    start_date=datetime(2025, 4, 28),
    schedule_interval="@daily",
    catchup=False,
    tags=["api", "s3", "snowflake", "gym"],
) as dag:

    def get_auth_token():
        resp = requests.post(
            f"{API_BASE_URL}/login", json={"username": "manu", "password": "manu@123"}, timeout=10
        )
        resp.raise_for_status()
        return resp.text.strip()

    def fetch_store_upload(api_name, endpoint, execution_date_str, token):
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{API_BASE_URL}{endpoint}?date={execution_date_str}"
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        dag_dir = os.path.dirname(os.path.abspath(__file__))
        tmp_dir = os.path.join(dag_dir, "tmp_data")
        os.makedirs(tmp_dir, exist_ok=True)
        file_name = f"{api_name}_{execution_date_str[:10]}.json"
        local_file_path = os.path.join(tmp_dir, file_name)

        with open(local_file_path, "w") as f:
            json.dump(data, f, indent=2)

        s3_key = f"gym-data/{api_name}/{file_name}"
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_hook.load_file(filename=local_file_path, key=s3_key, bucket_name=S3_BUCKET_NAME, replace=True)

        print(f"✅ Uploaded {file_name} to S3: s3://{S3_BUCKET_NAME}/{s3_key}")
        os.remove(local_file_path)
        print(f"🗑️ Deleted local file: {local_file_path}")

    def create_api_tasks(api_name, endpoint):
        def fetch_and_upload(**kwargs):
            execution_date = kwargs["execution_date"]
            execution_date_str = execution_date.strftime('%Y-%m-%dT%H:%M:%S')
            token = get_auth_token()
            fetch_store_upload(api_name, endpoint, execution_date_str, token)

        return PythonOperator(
            task_id=f"{api_name}_task",
            python_callable=fetch_and_upload,
            provide_context=True,
        )

    def cleanup_tmp_folder():
        dag_dir = os.path.dirname(os.path.abspath(__file__))
        tmp_dir = os.path.join(dag_dir, "tmp_data")
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
            print(f"🧹 Cleaned up temp directory: {tmp_dir}")

    cleanup_task = PythonOperator(
        task_id="cleanup_tmp_files",
        python_callable=cleanup_tmp_folder,
    )

    # ───────────────────────────────────────────────── #
    # ❄️ Snowflake MERGE Statements (No Duplicates!)
    # ───────────────────────────────────────────────── #
    copy_statements = {
        "user_account_details": """
        MERGE INTO user_account_details tgt
        USING (
            SELECT 
                $1:id::NUMBER AS id, 
                $1:loginId::STRING AS loginId,
                $1:emailId::STRING AS emailId,
                $1:password::STRING AS password,
                $1:membershipId::STRING AS membershipId,
                $1:personal_details_status::STRING AS personal_details_status,
                $1:health_details_status::STRING AS health_details_status,
                $1:createdTs::TIMESTAMP_TZ AS createdTs,
                $1:updatedTs::TIMESTAMP_TZ AS updatedTs
            FROM @gym_raw_stage/user_account_details/
            (FILE_FORMAT => 'json_format')
        ) src
        ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET
            loginId = src.loginId,
            emailId = src.emailId,
            password = src.password,
            membershipId = src.membershipId,
            personal_details_status = src.personal_details_status,
            health_details_status = src.health_details_status,
            createdTs = src.createdTs,
            updatedTs = src.updatedTs
        WHEN NOT MATCHED THEN INSERT (
            id, loginId, emailId, password, membershipId, personal_details_status,
            health_details_status, createdTs, updatedTs
        ) VALUES (
            src.id, src.loginId, src.emailId, src.password, src.membershipId,
            src.personal_details_status, src.health_details_status, src.createdTs, src.updatedTs
        );
        """,
        "user_personal_details": """
        MERGE INTO user_personal_details tgt
        USING (
            SELECT 
                $1:id::NUMBER AS id, 
                $1:loginId::STRING AS loginId, 
                $1:firstName::STRING AS firstName, 
                $1:lastName::STRING AS lastName, 
                $1:mobileNumber::STRING AS mobileNumber,
                $1:countryId::NUMBER AS countryId,
                $1:stateId::NUMBER AS stateId,
                $1:cityId::NUMBER AS cityId,
                $1:createdTs::TIMESTAMP_TZ AS createdTs,
                $1:updatedTs::TIMESTAMP_TZ AS updatedTs
            FROM @gym_raw_stage/user_personal_details/
            (FILE_FORMAT => 'json_format')
        ) src
        ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET
            loginId = src.loginId,
            firstName = src.firstName,
            lastName = src.lastName,
            mobileNumber = src.mobileNumber,
            countryId = src.countryId,
            stateId = src.stateId,
            cityId = src.cityId,
            createdTs = src.createdTs,
            updatedTs = src.updatedTs
        WHEN NOT MATCHED THEN INSERT (
            id, loginId, firstName, lastName, mobileNumber, countryId, stateId, cityId, createdTs, updatedTs
        ) VALUES (
            src.id, src.loginId, src.firstName, src.lastName, src.mobileNumber, src.countryId, src.stateId, src.cityId, src.createdTs, src.updatedTs
        );
        """,
        "user_health_details": """
        MERGE INTO user_health_details tgt
        USING (
            SELECT 
                $1:id::NUMBER AS id, 
                $1:loginId::STRING AS loginId,
                $1:age::NUMBER AS age,
                $1:gender::STRING AS gender,
                $1:height::NUMBER AS height,
                $1:currentWeight::NUMBER AS currentWeight,
                $1:goalWeight::NUMBER AS goalWeight,
                $1:activityLevel::STRING AS activityLevel,
                $1:targetCalories::NUMBER AS targetCalories,
                $1:createdTs::TIMESTAMP_TZ AS createdTs,
                $1:updatedTs::TIMESTAMP_TZ AS updatedTs
            FROM @gym_raw_stage/user_health_details/
            (FILE_FORMAT => 'json_format')
        ) src
        ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET
            loginId = src.loginId,
            age = src.age,
            gender = src.gender,
            height = src.height,
            currentWeight = src.currentWeight,
            goalWeight = src.goalWeight,
            activityLevel = src.activityLevel,
            targetCalories = src.targetCalories,
            createdTs = src.createdTs,
            updatedTs = src.updatedTs
        WHEN NOT MATCHED THEN INSERT (
            id, loginId, age, gender, height, currentWeight, goalWeight, activityLevel, targetCalories, createdTs, updatedTs
        ) VALUES (
            src.id, src.loginId, src.age, src.gender, src.height, src.currentWeight, src.goalWeight, src.activityLevel, src.targetCalories, src.createdTs, src.updatedTs
        );
        """,
        "message_center": """
        MERGE INTO message_center tgt
        USING (
            SELECT 
                $1:id::NUMBER AS id,
                $1:targetLoginId::STRING AS targetLoginId,
                $1:sourceLoginId::STRING AS sourceLoginId,
                $1:message::STRING AS message,
                $1:messageAcceptanceStatus::STRING AS messageAcceptanceStatus,
                $1:status::STRING AS status,
                $1:isRead::STRING AS isRead,
                $1:createdTs::TIMESTAMP_TZ AS createdTs,
                $1:updatedTs::TIMESTAMP_TZ AS updatedTs
            FROM @gym_raw_stage/message_center/
            (FILE_FORMAT => 'json_format')
        ) src
        ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET
            targetLoginId = src.targetLoginId,
            sourceLoginId = src.sourceLoginId,
            message = src.message,
            messageAcceptanceStatus = src.messageAcceptanceStatus,
            status = src.status,
            isRead = src.isRead,
            createdTs = src.createdTs,
            updatedTs = src.updatedTs
        WHEN NOT MATCHED THEN INSERT (
            id, targetLoginId, sourceLoginId, message, messageAcceptanceStatus, status, isRead, createdTs, updatedTs
        ) VALUES (
            src.id, src.targetLoginId, src.sourceLoginId, src.message, src.messageAcceptanceStatus, src.status, src.isRead, src.createdTs, src.updatedTs
        );
        """,
    }

    # ───────────────────────────────────────────────── #
    # ⛓️ Task Dependencies
    # ───────────────────────────────────────────────── #
    snowflake_tasks = []
    for api_name, endpoint in API_ENDPOINTS.items():
        api_task = create_api_tasks(api_name, endpoint)

        snowflake_task = SnowflakeOperator(
            task_id=f"load_{api_name}_to_snowflake",
            sql=copy_statements[api_name],
            snowflake_conn_id="snowflake_default",
        )

        api_task >> snowflake_task >> cleanup_task
        snowflake_tasks.append(snowflake_task)

    dbt_run = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /usr/local/airflow/include/snowflake_project && dbt run --project-dir .",
    )
    snowflake_tasks >> dbt_run
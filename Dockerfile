FROM quay.io/astronomer/astro-runtime:12.8.0
RUN pip install apache-airflow-providers-snowflake==4.1.0
RUN pip install dbt-core dbt-snowflake

COPY ./dbt-config /home/astro/.dbt
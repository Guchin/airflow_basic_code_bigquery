from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago, timedelta,datetime



default_args = {
                "start_date": days_ago(2),                            
                "retries": 3,
                "retry_delay": timedelta(minutes=5)}


with DAG("contactability",
         default_args=default_args,
         description="pipeline in Bigquery",
         schedule_interval="@daily",         
         catchup=False,
         template_searchpath="/home/airflow/gcs/dags/scripts" 
         
                  ) as dag:



    do_cleaning = BigQueryInsertJobOperator(
    task_id="contactability_base",
    configuration={"query": {"query": "{% include 'cleaning.sql' %}",
                             "useLegacySql": False}},
    location="US"    )
    
    
    build_model = BigQueryInsertJobOperator(
        task_id="contactability_analytics",
        configuration={"query": {"query": "{% include 'model.sql' %}",
                                "useLegacySql": False}},
        location="US"
    )

do_cleaning >> build_model
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from custom_ingestion.processor.ingestion_processor import IngestionProcessor
from custom_ingestion.model.config_variables import ConfigVariables



config = ConfigVariables()  # noqa
ingestion_processor = IngestionProcessor(config)

def stage_process():
    ingestion_processor.stage_processor()

def bronze_process():
    ingestion_processor.bronze_processor()

def silver_process():
    ingestion_processor.silver_processor()

def gold_dimensions_process():
    ingestion_processor.gold_dimensions_processor()

def gold_facts_process():
    ingestion_processor.gold_facts_processor()


dag =  DAG(
    dag_id="airflow_dag_ingestion_orchestrator",
    schedule='@daily',
    start_date=datetime(2025, 2, 10),
    catchup=False,
    tags=["delta", "duckdb", "medallion"]
)

stage_task = PythonOperator(
    task_id="stage_process",
    python_callable=stage_process,
    dag=dag
)

bronze_task = PythonOperator(
    task_id="bronze_process",
    python_callable=bronze_process,
    dag=dag
)

silver_task = PythonOperator(
    task_id="silver_process",
    python_callable=silver_process,
    dag=dag
)

gold_dimensions_task = PythonOperator(
    task_id="gold_dimensions_process",
    python_callable=gold_dimensions_process,
    dag=dag
)

gold_facts_task = PythonOperator(
    task_id="gold_facts_process",
    python_callable=gold_facts_process,
    dag=dag
)

stage_task >> bronze_task >> silver_task >> gold_dimensions_task >> gold_facts_task

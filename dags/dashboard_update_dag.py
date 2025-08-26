import json

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

default_args={
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=1),
}

kst_tz=timezone(timedelta(hours=9))

with DAG(
    dag_id="sync_batch_job_dag",
    default_args=default_args,
    start_date=datetime(2025, 8, 24, tzinfo=kst_tz),
    schedule="@daily",
    catchup=False,
    tags=["batch", "api"],
) as dag:
    daily_data_update_job = HttpOperator(
        task_id="daily_data_update_job",
        http_conn_id="spring_boot_api",
        method="POST",
        endpoint="/consulting/batch/",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "jobName": "dailyDataUpdateJob"
        }),
        log_response=True
    )

    daily_profit_job = HttpOperator(
        task_id="daily_profit_job",
        http_conn_id="spring_boot_api",
        method="POST",
        endpoint="/consulting/batch/",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "jobName": "dailyProfitJob"
        }),
        log_response=True,
    )

    monthly_profit_status_job = HttpOperator(
        task_id="monthly_profit_status_job",
        http_conn_id="spring_boot_api",
        method="POST",
        endpoint="/consulting/batch/",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "jobName": "monthlyProfitStatusJob"
        }),
        log_response=True,
    )

    progress_group_job = HttpOperator(
        task_id="progress_group_job",
        http_conn_id="spring_boot_api",
        method="POST",
        endpoint="/consulting/batch/",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "jobName": "progressGroupJob"
        }),
        log_response=True,
    )

    student_lecture_count_job = HttpOperator(
        task_id="student_lecture_count_job",
        http_conn_id="spring_boot_api",
        method="POST",
        endpoint="/consulting/batch/",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "jobName": "studentLectureCountJob"
        }),
        log_response=True,
    )

    complete_progress_job = HttpOperator(
        task_id="complete_progress_job",
        http_conn_id="spring_boot_api",
        method="POST",
        endpoint="/consulting/batch/",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "jobName": "completeProgressJob"
        }),
        log_response=True,
    )

    daily_data_update_job >> [daily_profit_job, monthly_profit_status_job, progress_group_job, student_lecture_count_job, complete_progress_job]
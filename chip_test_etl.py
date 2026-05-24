"""
Chip Test Daily ETL DAG
Orchestrates the full pipeline: S3 → Glue Spark → Glue Catalog → Athena

This runs locally in Docker — credentials come from your AWS connection in Airflow UI.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


# ─── CONFIG — Change these to YOUR bucket names ─────────
RAW_BUCKET = "nvidia-demo-raw-srikanth-2026"
PROCESSED_BUCKET = "nvidia-demo-processed-srikanth-2026"
DLQ_BUCKET = "nvidia-demo-dlq-srikanth-2026"
RESULTS_BUCKET = "nvidia-demo-results-srikanth-2026"
GLUE_JOB_NAME = "chip_test_processor"
CRAWLER_NAME = "chip_test_crawler"
ATHENA_DATABASE = "nvidia_demo"


default_args = {
    "owner": "srikanth_kankati",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def check_s3_data(**context):
    """Verify raw data exists in S3."""
    import boto3
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix="chip_tests/")
    count = response.get("KeyCount", 0)
    print(f"Found {count} objects in s3://{RAW_BUCKET}/chip_tests/")
    if count == 0:
        raise ValueError("No raw data found in S3")
    return count


def trigger_glue_job(**context):
    """Start the Glue PySpark job."""
    import boto3
    glue = boto3.client("glue")
    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--RAW_BUCKET": RAW_BUCKET,
            "--PROCESSED_BUCKET": PROCESSED_BUCKET,
            "--DLQ_BUCKET": DLQ_BUCKET,
        }
    )
    run_id = response["JobRunId"]
    print(f"Started Glue job run: {run_id}")
    return run_id


def wait_for_glue_job(**context):
    """Poll until the Glue job completes."""
    import boto3, time
    run_id = context["ti"].xcom_pull(task_ids="trigger_glue_job")
    glue = boto3.client("glue")
    
    while True:
        response = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)
        state = response["JobRun"]["JobRunState"]
        print(f"Glue job state: {state}")
        if state in ["SUCCEEDED"]:
            return "success"
        if state in ["FAILED", "ERROR", "TIMEOUT", "STOPPED"]:
            raise ValueError(f"Glue job failed with state: {state}")
        time.sleep(30)


def trigger_crawler(**context):
    """Run the Glue Crawler to refresh the catalog."""
    import boto3
    glue = boto3.client("glue")
    try:
        glue.start_crawler(Name=CRAWLER_NAME)
        print(f"Started crawler: {CRAWLER_NAME}")
    except glue.exceptions.CrawlerRunningException:
        print("Crawler already running, skipping start")
    return "started"


def run_athena_summary(**context):
    """Run a summary query against the processed data."""
    import boto3, time
    athena = boto3.client("athena")
    
    query = f"""
        SELECT fab, 
               COUNT(*) as total,
               ROUND(AVG(CASE WHEN passed THEN 1.0 ELSE 0.0 END) * 100, 2) as yield_pct
        FROM {ATHENA_DATABASE}.chip_tests
        GROUP BY fab
        ORDER BY yield_pct ASC
    """
    
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": f"s3://{RESULTS_BUCKET}/athena/"}
    )
    query_id = response["QueryExecutionId"]
    print(f"Started Athena query: {query_id}")
    
    # Wait for completion
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            print("Athena query succeeded")
            return query_id
        if state in ["FAILED", "CANCELLED"]:
            raise ValueError(f"Athena query failed: {state}")
        time.sleep(5)


def notify_complete(**context):
    """Log pipeline completion."""
    print("✅ Pipeline completed successfully!")
    print(f"Execution date: {context['ds']}")
    return "done"


with DAG(
    dag_id="chip_test_daily_etl",
    description="Daily ETL for semiconductor chip test data",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=default_args,
    tags=["nvidia-demo", "chip-tests", "etl"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    check_data = PythonOperator(
        task_id="check_s3_data",
        python_callable=check_s3_data,
    )
    
    trigger_spark = PythonOperator(
        task_id="trigger_glue_job",
        python_callable=trigger_glue_job,
    )
    
    wait_spark = PythonOperator(
        task_id="wait_for_glue_job",
        python_callable=wait_for_glue_job,
    )
    
    refresh_catalog = PythonOperator(
        task_id="refresh_glue_catalog",
        python_callable=trigger_crawler,
    )
    
    summary = PythonOperator(
        task_id="run_athena_summary",
        python_callable=run_athena_summary,
    )
    
    done = PythonOperator(
        task_id="notify_complete",
        python_callable=notify_complete,
    )
    
    end = EmptyOperator(task_id="end")
    
    # Dependencies — defines the order
    start >> check_data >> trigger_spark >> wait_spark >> refresh_catalog >> summary >> done >> end

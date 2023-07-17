from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

# Define the DAG
dag = DAG(
    dag_id="spark_dag",
    schedule_interval="0 * * * *",  # Run every hour at the beginning of the hour
    start_date=datetime.now() - timedelta(hours=1),
    catchup=False

)

# Define the SparkSubmitOperator
spark_task = SparkSubmitOperator(
    task_id="spark_submit_task",
    application="C:\Users\ravikumar.g\PycharmProjects\CPE-12-7\src\workflow\cpe_spark.py",
    conn_id="spark_default",
    # conf={"spark.master": "spark://your_spark_master"},
    dag=dag
)

spark_task
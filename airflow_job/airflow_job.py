from datetime import datetime, timedelta
import uuid

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 2),
}

with DAG(
    dag_id="flight_booking_cluster_dataproc_dag",
    default_args=default_args,
    schedule_interval=None,  # Run manually or via trigger
    catchup=False,
) as dag:

    # Load environment-specific variables
    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-project-gdsf")
    bq_project = Variable.get("bq_project", default_var="aerial-gadget-458900-f7")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
    tables = Variable.get("tables", deserialize_json=True)

    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    CLUSTER_NAME = f"flight-booking-cluster-{uuid.uuid4().hex[:8]}"
    REGION = "us-central1"
    PROJECT_ID = "aerial-gadget-458900-f7"

    # Task 1: Wait for input file in GCS
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=gcs_bucket,
        object=f"airflow-project1/source-{env}/flight_booking.csv",
        google_cloud_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    # Task 2: Create Dataproc Cluster with Autoscaling
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-4",
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-4",
            },
            "autoscaling_config": {
                "policy_uri": f"projects/{PROJECT_ID}/regions/{REGION}/autoscalingPolicies/flight-autoscale-policy"
            },
        },
    )

    # Task 3: Submit the Spark job to the cluster
    submit_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{gcs_bucket}/airflow-project1/spark-job/spark_transformation_job.py",
                "args": [
                    f"--env={env}",
                    f"--bq_project={bq_project}",
                    f"--bq_dataset={bq_dataset}",
                    f"--transformed_table={transformed_table}",
                    f"--route_insights_table={route_insights_table}",
                    f"--origin_insights_table={origin_insights_table}",
                ],
            },
        },
    )

    # Task 4: Delete the Dataproc Cluster after job
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",  # Always attempt to delete
    )

    # DAG Dependencies
    file_sensor >> create_cluster >> submit_job >> delete_cluster

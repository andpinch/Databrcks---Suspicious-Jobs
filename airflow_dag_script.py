import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Mapping, Union
from dags.nas_high_frequency_jobs.lib.databricks_analyzer import DatabricksAnalyzer

default_args = {
    'owner': 'Airflow',
    'email': 'pinchuk.a@pg.com',
    'depends_on_past': False,
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False
}

dag = DAG(
    dag_id='NAS_HIGH_FREQUENCY_JOBS',
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
    default_args=default_args,
    concurrency=1,
)

variables = "NAS_HIGH_FREQUENCY_JOBS"
dag_config_json = Variable.get(variables, deserialize_json=True)

user_email = dag_config_json['user'] + "@pg.com"
email_recipients = dag_config_json["emailRecipients"]
email_recipients.append(user_email)
sender_email = 'neighborhood.im@pg.com'

def detect_suspicious_jobs(**kwargs):
    databricks_connection = BaseHook.get_connection(dag_config_json["databricksConnId"])
    token = databricks_connection.password
    api_endpoint = databricks_connection.host

    analyzer = DatabricksAnalyzer(api_endpoint, token)
    result = analyzer.detect_high_frequency_jobs()

    exceeded_duration = result.get('exceeded_duration', [])
    frequent_job_runs = result.get('frequent_job_runs', [])

    ti = kwargs['ti']
    email_body_content = analyzer.compose_email_body(exceeded_duration, frequent_job_runs)

    ti.xcom_push(key='email_body_content', value=email_body_content)

def send_email(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='detect_suspicious_jobs', key='detection_result')

    print("Retrieved Result:", result)

    exceeded_duration = result.get('exceeded_duration')
    frequent_job_runs = result.get('frequent_job_runs')

    databricks_connection = BaseHook.get_connection(dag_config_json["databricksConnId"])
    token = databricks_connection.password
    api_endpoint = databricks_connection.host

    analyzer = DatabricksAnalyzer(api_endpoint, token)

    email_body_content = analyzer.compose_email_body(exceeded_duration, frequent_job_runs)

    subject_template = 'NAS_HIGH_FREQUENCY_JOBS has been done.'
    html_content_template = """
        <p>Job Runs Exceeding Duration:</p>
        <pre>{{ exceeded_duration }}</pre>

        <p>Frequent Job Runs:</p>
        <pre>{{ frequent_job_runs }}</pre>
    """

    return {"subject": subject_template, "html_content": html_content_template, "email_body_content": email_body_content}

send_email_success_task = EmailOperator(
    task_id="send_email_success",
    to=email_recipients,
    subject=f"NAS_HIGH_FREQUENCY_JOBS has been done",
    html_content="",
    trigger_rule="all_success"
)

send_email_failure_task = EmailOperator(
    task_id="send_email_failure",
    to=email_recipients,
    subject=f"NAS_HIGH_FREQUENCY_JOBS has failed",
    html_content="",
    trigger_rule="one_failed"
)

with dag:
    Start = DummyOperator(task_id='Start')
    End = DummyOperator(task_id='End', trigger_rule=TriggerRule.NONE_FAILED)

    detect_suspicious_jobs_task = PythonOperator(
        task_id='detect_suspicious_jobs',
        python_callable=detect_suspicious_jobs,
        provide_context=True,
    )

    send_email_task = EmailOperator(
        task_id='send_email',
        to=email_recipients,
        subject="NAS_HIGH_FREQUENCY_JOBS has been done",
        html_content="<pre>{{ ti.xcom_pull(task_ids='detect_suspicious_jobs', key='email_body_content') }}</pre>",
        trigger_rule="all_success"
    )

    Start >> detect_suspicious_jobs_task >> send_email_task >> End
    End >> [send_email_success_task, send_email_failure_task]

from datetime import datetime, timedelta
from airflow import DAG, utils
from airflow.operators.papermill_operator import PapermillOperator

# Define default_args dictionary to set the default parameters of the DAG
default_args = {
    "owner": "airflow",
    "start_date": utils.dates.days_ago(0),
    "retries": 0,
    "catchup": False,
}

# Define the DAG
dag = DAG(
    "execute_notebook",
    default_args=default_args,
    description="DAG to execute a Jupyter Notebook using Papermill",
)

# Define the path to your Jupyter Notebook
notebook_path = "/opt/airflow/notebooks/hello_notebook.ipynb"

# Define the parameters to pass to the notebook
notebook_parameters = {
    "param1": "value1",
    "param2": "value2",
}

# Define the PapermillOperator task
execute_notebook_task = PapermillOperator(
    task_id="execute_notebook_task",
    input_nb=notebook_path,
    output_nb=notebook_path,  # Overwrite the input notebook with the executed notebook
    dag=dag,
)

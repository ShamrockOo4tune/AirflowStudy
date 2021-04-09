from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from util.hw3_functions import download_titanic_dataset_2,\
                               pivot_dataset_3,\
                               mean_fare_per_class_3

from util.settings import default_settings

with DAG(**default_settings()) as dag:
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,)

    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset_2,
        dag=dag,)
    
    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_dataset',
        python_callable=pivot_dataset_3,
        dag=dag,)
    
    mean_fares_titanic_dataset = PythonOperator(
        task_id='mean_fares_dataset',
        python_callable=mean_fare_per_class_3,
        dag=dag,)
        
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ ds }}"',
        dag=dag,)

    first_task >> create_titanic_dataset >> (pivot_titanic_dataset, mean_fares_titanic_dataset) >> last_task

import os
import pandas as pd
from airflow.hooks.base import BaseHook
#from airflow.hooks.base_hook import BaseHook


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')


def download_titanic_dataset_2(**kwargs):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    kwargs['task_instance'].xcom_push('titanic_df', df.to_json())


def pivot_dataset_2(**kwargs):
    titanic_json = kwargs['task_instance'].xcom_pull(task_ids='download_titanic_dataset',
                                                     key='titanic_df')
    titanic_df = pd.read_json(titanic_json)
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values=['Name'],
                                aggfunc='count').reset_index()
    pg_hook = BaseHook.get_hook('postgres_default')
    # pg_hook.insert_rows а вот дальше что не знаю


def mean_fare_per_class_2(**kwargs):
    titanic_json = kwargs['task_instance'].xcom_pull(task_ids='download_titanic_dataset',
                                                     key='titanic_df')
    titanic_df = pd.read_json(titanic_json)
    df = titanic_df.pivot_table(columns=['Pclass'],
                                values='Fare',
                                aggfunc='mean').reset_index()
    pg_hook = BaseHook.get_hook('postgres_default')
    # pg_hook.insert_rows а вот дальше что не знаю


def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values=['Name'],
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))


def mean_fare_per_class():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(columns=['Pclass'],
                                values='Fare',
                                aggfunc='mean').reset_index()
    df.to_csv(get_path('titanic_mean_fares.csv'))

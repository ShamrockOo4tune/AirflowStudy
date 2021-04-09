import os
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import Variable


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


def pivot_dataset_3(**kwargs):

    # вытягиваем датафрейм сериализованный в json из xcom:
    titanic_json = kwargs['task_instance'].xcom_pull(task_ids='download_titanic_dataset',
                                                     key='titanic_df')
    # преобразуем в pandas dataframe и изменяем агрегацией:
    titanic_df = pd.read_json(titanic_json)
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()

    # создаем кастом хук, коннектшн берем из предварительно созданного в UI:
    pg_hook = BaseHook.get_hook('postgres_default')

    # имя таблицы в локальной БД предварительно задано в UI в Variables. Извлекаем:
    pg_table_name = Variable.get('pivot_table_name')

    # перемалываем датафрейм в список кортежей, приводим типы к стандартным (str и int):
    pg_rows = list(df.to_records(index=False))
    pg_rows_conv = [(t[0], int(t[1]), int(t[2]), int(t[3])) for t in pg_rows]

    # извлекаем названия полей(колонок) датафрейма и приводим их типы к строковому:
    pg_columns = list(df.columns)
    pg_columns_conv = [pg_columns[0],
                       '"' + str(pg_columns[1]) + '"',
                       '"' + str(pg_columns[2]) + '"',
                       '"' + str(pg_columns[3]) + '"']

    # отправляем данные в локальную БД:
    pg_hook.insert_rows(table=pg_table_name,
                        rows=pg_rows_conv,
                        target_fields=pg_columns_conv,
                        commit_every=0,
                        replace=False)


def mean_fare_per_class_2(**kwargs):
    titanic_json = kwargs['task_instance'].xcom_pull(task_ids='download_titanic_dataset',
                                                     key='titanic_df')
    titanic_df = pd.read_json(titanic_json)
    df = titanic_df.pivot_table(columns=['Pclass'],
                                values='Fare',
                                aggfunc='mean').reset_index()


def mean_fare_per_class_3(**kwargs):

    # вытягиваем датафрейм сериализованный в json из xcom:
    titanic_json = kwargs['task_instance'].xcom_pull(task_ids='download_titanic_dataset',
                                                     key='titanic_df')
    # преобразуем в pandas dataframe и изменяем группировками, агрегациями:
    titanic_df = pd.read_json(titanic_json)
    df = titanic_df\
        .groupby(['Pclass'])\
        .agg({'Fare': 'mean'})\
        .reset_index()

    # создаем кастом хук, коннектшн берем из предварительно созданного в UI:
    pg_hook = BaseHook.get_hook('postgres_default')

    # имя тааблицы в локальной БД предварительно задано в UI в Variables. Извлекаем:
    pg_table_name = Variable.get('mean_fares_table_name')

    # перемалываем датафрейм в список кортежей, приводим типы к стандартным (int и float):
    pg_rows = list(df.to_records(index=False))
    pg_rows_conv = [(int(t[0]), float(t[1])) for t in pg_rows]

    # извлекаем названия полей(колонок) датафрейма:
    pg_columns = list(df.columns)

    # отправляем данные в локальную БД:
    pg_hook.insert_rows(table=pg_table_name,
                        rows=pg_rows_conv,
                        target_fields=pg_columns,
                        commit_every=0,
                        replace=False)


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

# Создает виртуальное окружение и удаляет его после выполнения задач
# При каждом запуске будет создаваться venv, устанавливаться библиотеки и удаляться
# Должно быть установлено в контейнере расширение для airflow-venv

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import pendulum

local_tz = pendulum.timezone("Europe/Moscow")

# Настройка параметров DAG
default_args = {
    'owner': 'Евгений',
    'start_date': datetime(2024, 4, 1, 10, 0, 0, tzinfo=local_tz), # Дата старта, но не первого запуска!
    'email': ['example@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# Определение DAG
dag = DAG(
    dag_id='Tutorial_PythonOperator',
    default_args=default_args,
    description='Turorial for students',
    max_active_runs=1,
    catchup=False,
    schedule_interval='0 10 5 * *',
    tags=['part_1']
)

###################################################################################################

def foo_1():
    print("Это наша первая задача!")

def foo_2(x,y):
    print(f"Это наша вторая задача! Ответ: {x+y}")

script_1 = PythonOperator(
    task_id='first_task',
    python_callable=foo_1,
    dag=dag
)

script_2 = PythonOperator(
    task_id='second_task',
    python_callable=foo_2,
    op_kwargs={"x":10, "y":300}, # здесь передаются "kwargs"
    dag=dag
)


# Установка зависимости между задачами
script_1 >> script_2

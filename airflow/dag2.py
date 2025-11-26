from airflow import DAG # класс для создания графа
from datetime import datetime, timedelta # для задания дат
from airflow.operators.dummy_operator import DummyOperator # для начала и конца дага
from airflow.providers.postgres.operators.postgres import PostgresOperator # для вызова функций
from airflow.utils.task_group import TaskGroup # для создания групп тасков

DB_CONN = "gp_std14_135" # ID соединения с Greenplum

# ПАРАМЕТРЫ DAG
default_args = {
    'owner': 'std14_135', # владелец
    'depends_on_past': False, # запуск вне зависимости от статуса пред задачи
    'start_date': datetime(2025, 4, 7), # дата начала (не особо важна в данном случае, т.к. этот даг будет запускаться только вручную (см. schedule_interval)
    'retries': 1, # кол-во попыток перезапуска при ошибке
    'retry_delay': timedelta(minutes=5), # задержка между попытками перезапуска (5 мин)
}

# ОБЪЯВЛЕНИЕ DAG
with DAG(
    'std14_135_main_dag', # имя дага
    default_args=default_args, # настройки по умолчанию
    description='Main DAG for loading data into Greenplum', # описание дага - загрухка данных в Greenplum
    schedule_interval=None,  # пустой интервал расписания запуска = запуск вручную
    catchup=False, # не запускать пропущенные интервалы
) as dag:
    
# НАЧАЛО DAG
    task_start = DummyOperator(task_id="start", dag=dag) # пустая задача для начала дага

# ЗАДАЧА 1: Цикличная загрузка справочников
    with TaskGroup("load_reference_tables") as load_ref_tables: # создадим группу тасков load_reference_tables и обозначим ее как load_ref_tables
        for table in ['region', 'chanel', 'product', 'price']: # цикл по списку таблиц
            task = PostgresOperator( # для каждой таблицы в списке создается задача
                    task_id=f"load_{table}", # имя задачи load_region\chanel\price и т.д.
                    postgres_conn_id=DB_CONN, # ID соединения с Greenplum (было указано вначале)
                    sql=f"SELECT std14_135.g_load_full('std14_135.{table}', 'std14_135.{table}_gpfdist_ext');", # вызов функции g_load_full
                    dag=dag,
                    )

# ЗАДАЧА 2.1: Загрузка данных в таблицы-факты - sales
    task_load_sales = PostgresOperator(
        task_id="load_fact_sales",
        postgres_conn_id=DB_CONN,
        sql=f"SELECT std14_135.f_load_du('std14_135.sales', 'std14_135.sales_pxf_ext', 'date');",
        dag=dag,
        )

# ЗАДАЧА 2.2: Загрузка данных в таблицы-факты - plan
    task_load_plan = PostgresOperator(
        task_id="load_fact_plan",
        postgres_conn_id=DB_CONN,
        sql=f"SELECT std14_135.f_load_du('std14_135.plan', 'std14_135.plan_pxf_ext', 'date');",
        dag=dag,
        )

# ЗАДАЧА 3: Расчёт витрины
    task_mart = PostgresOperator(
        task_id="mart",
        postgres_conn_id=DB_CONN,
        sql="SELECT std14_135.mart_sales_res('202104');",  # месяц расчета витрины - апрель
        dag=dag,
        )

# КОНЕЦ DAG
    task_end = DummyOperator(task_id="end", dag=dag)

# ГРАФ ПОСЛЕДОВАТЕЛЬНОСТИ
    task_start >> load_ref_tables >> task_load_sales >> task_load_plan >> task_mart >> task_end

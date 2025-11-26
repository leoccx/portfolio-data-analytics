from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup  # для создания групп тасков

DB_CONN = "std14_135_fp"

default_args = {
    'depends_on_past': False,  # запуск вне зависимости от статуса пред задачи
    'owner': 'std14_135',
    'start_date': datetime(2025, 1, 1),  # поставлю дату из прошлого + catchup=False, чтобы DAG запустился сейчас (не перезапуская при этом историю), а не ждал след интервала
    'retries': 1,  # кол-во попыток перезапуска при ошибке
    'retry_delay': timedelta(minutes=5),  # задержка между попытками запуска 5 мин
}

# объявление DAG

with DAG(
    "fp_dag",
    schedule_interval='0 0 * * *',  # cron формат: запуск ежедневно в полночь
    default_args=default_args,
    catchup=False,  # не запускать пропущенные интервалы
    description='ELT-пайплайн для загрузки фактов/справочников и сборки витрины',
) as dag:
    # 0. Начало DAG

    task_start = DummyOperator(task_id="start", dag=dag)

    # 1.1. FULL-загрузка справочников stores, promos, promo_types

    with TaskGroup(
        "load_reference_tables"
    ) as load_ref_tables:  # создадим группу тасков load_reference_tables и обозначим ее как load_ref_tables
        for table in ['stores', 'promos', 'promo_types']:  # цикл по списку таблиц
            task = PostgresOperator(  # для каждой таблицы в списке создается задача
                task_id=f"load_ref_{table}",  # имя задачи load_ref_stores/promos и т.д.
                postgres_conn_id=DB_CONN,  # ID соединения с Greenplum (было указано в начале)
                sql=f"SELECT std14_135_fp.f_load_full('std14_135_fp.{table}', 'std14_135_fp.{table}_ext');",  # вызов функции f_load_full
                dag=dag,
            )

    # 1.2. Отдельная full-загрузка маленьких событийных таблиц coupons, traffic (т.к. при загрузке нужно изменить типы столбцов)
    
    task_load_f_coupons = PostgresOperator(
    task_id="load_f_coupons",
    postgres_conn_id=DB_CONN,
    sql="""
        TRUNCATE TABLE std14_135_fp.coupons1;
        INSERT INTO std14_135_fp.coupons1
        SELECT
            plant,
            to_date(day, 'YYYYMMDD'),
            number,
            id_promo,
            product,
            cheque,
            REPLACE(discount, ',', '.')::numeric(10,2)
        FROM std14_135_fp.coupons_ext;
    """
)

    task_load_f_traffic = PostgresOperator(
    task_id="load_f_traffic",
    postgres_conn_id=DB_CONN,
    sql="""
        TRUNCATE TABLE std14_135_fp.traffic1;
        INSERT INTO std14_135_fp.traffic1
        SELECT
            plant,
            to_date("date", 'DD.MM.YYYY'),
            to_timestamp("time", 'HH24MISS'),
            frame_id,
            quantity
        FROM std14_135_fp.traffic_ext;
    """
)
            
    # 2. Delta Partition загрузка в большие факт-таблицы

    task_load_f_bills_head = PostgresOperator(
        task_id="load_f_bills_head",
        postgres_conn_id=DB_CONN,
        sql="""
            SELECT std14_135_fp.f_load_delta_partition(
                'std14_135_fp.bills_head1',
                'gp.bills_head',
                'calday',
                date '{{ ds }}',
                date '{{ next_ds }}',
                'intern',
                'intern'
            );
        """,
        dag=dag,
    )

    task_load_f_bills_item = PostgresOperator(
        task_id="load_f_bills_item",
        postgres_conn_id=DB_CONN,
        sql="""
            SELECT std14_135_fp.f_load_delta_partition(
                'std14_135_fp.bills_item1',
                'gp.bills_item',
                'calday',
                date '{{ ds }}',
                date '{{ next_ds }}',
                'intern',
                'intern'
            );
        """,
        dag=dag,
    )

    # 3. Сборка витрины

    task_mart = PostgresOperator(
        task_id='build_mart',
        postgres_conn_id=DB_CONN,
        sql="""
            SELECT std14_135_fp.f_upd_mart(
                'std14_135_fp.mart_store_day',
                'calday',
                date '{{ ds }}',
                date '{{ next_ds }}'
            );
        """,
        dag=dag,
    )

    # 4. Конец DAG

    task_end = DummyOperator(task_id="end", dag=dag)

    # 5. Граф последовательности

    (
        task_start
        >> load_ref_tables
        >> task_load_f_coupons
        >> task_load_f_traffic
        >> task_load_f_bills_head
        >> task_load_f_bills_item
        >> task_mart
        >> task_end
    )

    # ставим bills_item после ref (coupons) и после bills_head, т.к. она зависит от них

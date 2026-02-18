from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import oracledb
import duckdb
import os

# КОНФИГУРАЦИЯ

# путь к файлу DuckDB (в папке logs, которая смонтирована в Docker)

DUCKDB_PATH = '/opt/airflow/logs/vitrinas.duckdb'

# имя таблицы в DuckDB

TABLE_NAME = 'contracts_2025' # кратко - c2025

# имя таблицы для статистики качества

STATS_TABLE_NAME = 'dq_statistics_c2025'

# размер чанка для чтения из Oracle (чтоб не упасть по памяти)
# читаем по 50к строк

CHUNK_SIZE = 50000

# SQL ЗАПРОС С ПРОВЕРКАМИ КАЧЕСТВА

ORACLE_QUERY = """
SELECT 
.
.
.
"""

# ФУНКЦИИ

def refresh_vitrina_from_oracle(**context):
    """
    Чтение данных из Oracle + запись в DuckDB с проверками качества
    """
    
    print("НАЧАЛО ОБНОВЛЕНИЯ ВИТРИНЫ")
    
# 1. получаем подключение к Oracle

    print("Получаем подключение к Oracle...")
    oracle_conn = BaseHook.get_connection("oracle_main")
    oracle_url = (
        f"oracle+oracledb://{oracle_conn.login}:{oracle_conn.password}@"
        f"{oracle_conn.host}:{oracle_conn.port}/"
        f"?service_name={oracle_conn.extra_dejson.get('service_name', '')}"
    )

    print(f"Oracle URL: {oracle_conn.host}:{oracle_conn.port}")
    
# 2. подключаемся к DuckDB

    print(f"Подключаемся к DuckDB: {DUCKDB_PATH}...")

    con = duckdb.connect(DUCKDB_PATH)

    print("DuckDB подключен")
    
# 3. создаем таблицу витрины (если не существует)

    print(f"Создаем таблицу {TABLE_NAME}...")

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            contract20 INTEGER,
            id BIGINT,
            registry_number VARCHAR,
            registry_number_oos VARCHAR,
            procedure_registry_number VARCHAR,
            customer_name VARCHAR,
            customer_inn VARCHAR,
            grbs VARCHAR,
            complex_name VARCHAR,
            contract_name VARCHAR,
            lot_number VARCHAR,
            basis_name VARCHAR,
            status VARCHAR,
            edition_number VARCHAR,
            cost_in_ruble DOUBLE,
            purchase_sum_2024 DOUBLE,
            purchase_sum_2025 DOUBLE,
            purchase_sum_2026 DOUBLE,
            purchase_sum_2027 DOUBLE,
            participant_place VARCHAR,
            contract_price_at_conclusion DOUBLE,
            duration_end_date VARCHAR,
            paid_in_ruble DOUBLE,
            paid_percent DOUBLE,
            conclusion_date VARCHAR,
            registry_date VARCHAR,
            execution_start_date VARCHAR,
            execution_end_date VARCHAR,
            supplier_name VARCHAR,
            supplier_inn VARCHAR,
            supplier_kpp VARCHAR,
            region_code VARCHAR,
            region_name VARCHAR,
            law_type_lots VARCHAR,
            law_type_contracts VARCHAR,
            kpgz_name VARCHAR,
            kpgz_2_code VARCHAR,
            kpgz_2_name VARCHAR,
            kpgz_3_code VARCHAR,
            kpgz_3_name VARCHAR,
            method_of_supplier VARCHAR,
            reason_method_of_supplier_id VARCHAR,
            lot_id VARCHAR,
            law_223_reason VARCHAR,
            lot_registry_number VARCHAR,
            state VARCHAR,
            dq_priority_1 VARCHAR,
            dq_priority_2 VARCHAR,
            updated_at TIMESTAMP
        )
    """)

    print("Таблица создана/проверена")
    
    # 4. очистка витрины перед загрузкой

    print(f"Очищаем таблицу {TABLE_NAME}...")

    con.execute(f"DELETE FROM {TABLE_NAME}")

    print("Таблица очищена")
    
    # 5. читаем данные из Oracle чанками и записываем в DuckDB

    print(f"Читаем данные из Oracle (чанки по {CHUNK_SIZE} строк)...")

    total_rows = 0
    chunk_num = 0
    
    for chunk in pd.read_sql(ORACLE_QUERY, oracle_url, chunksize=CHUNK_SIZE):
        chunk_num += 1
        total_rows += len(chunk)
        
        # добавляем timestamp обновления

        chunk['updated_at'] = datetime.now()
        
        # переименовываем колонки для совместимости (если нужно)

        # chunk.columns = [col.lower().replace(' ', '_') for col in chunk.columns]
        
        # записываем чанк в DuckDB

        con.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM chunk")
        
        print(f"Чанк №{chunk_num}: {len(chunk)} строк (всего: {total_rows:,})")
    
    print(f"Всего загружено: {total_rows:,} строк")
    
    # 6. создаем таблицу статистики (если не существует)

    print(f"создаем таблицу {STATS_TABLE_NAME}...")

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {STATS_TABLE_NAME} (
            total_rows INTEGER,
            clean_rows INTEGER,
            priority_1_count INTEGER,
            priority_2_count INTEGER,
            clean_percent DOUBLE,
            check_date TIMESTAMP
        )
    """)

    print("таблица статистики создана/проверена")
    
    # 7. считаем статистику качества данных

    print("считаем статистику качества...")

    stats = con.execute(f"""
        SELECT 
            COUNT(*) AS total_rows,
            SUM(CASE WHEN dq_priority_1 = '' AND dq_priority_2 = '' AND dq_priority_3 = '' THEN 1 ELSE 0 END) AS clean_rows,
            SUM(CASE WHEN dq_priority_1 != '' THEN 1 ELSE 0 END) AS priority_1_count,
            SUM(CASE WHEN dq_priority_2 != '' THEN 1 ELSE 0 END) AS priority_2_count,
            ROUND(
                SUM(CASE WHEN dq_priority_1 = '' AND dq_priority_2 = '' AND dq_priority_3 = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                2
            ) AS clean_percent
        FROM {TABLE_NAME}
    """).fetchone()
    
    total_rows, clean_rows, p1, p2, clean_pct = stats
    
    # записываем статистику

    con.execute(f"""
        INSERT INTO {STATS_TABLE_NAME} 
        VALUES ({total_rows}, {clean_rows}, {p1}, {p2}, {p3}, {clean_pct}, NOW())
    """)

    print(f"Статистика: {total_rows:,} всего, {clean_rows:,} чистых ({clean_pct}%)")
    
    # 8. индексы

    print("Создаем индексы...")

    con.execute(f"CREATE INDEX IF NOT EXISTS idx_status ON {TABLE_NAME} (status)")
    con.execute(f"CREATE INDEX IF NOT EXISTS idx_conclusion_date ON {TABLE_NAME} (conclusion_date)")
    con.execute(f"CREATE INDEX IF NOT EXISTS idx_dq_priority_1 ON {TABLE_NAME} (dq_priority_1)")
    con.execute(f"CREATE INDEX IF NOT EXISTS idx_customer_inn ON {TABLE_NAME} (customer_inn)")

    print("Индексы созданы")
    
    # 9. закрываем подключение

    con.close()
    print("DuckDB отключен")
    
    # 10. возвращаем статистику

    dq_stats = {
        'total_rows': total_rows,
        'clean_rows': clean_rows,
        'priority_1': p1,
        'priority_2': p2,
        'clean_percent': clean_pct
    }
    
    print("=" * 60)
    print("ВИТРИНА ОБНОВЛЕНА УСПЕШНО!")
    print("=" * 60)
    print(f"Итоговая статистика: {dq_stats}")
    
    return dq_stats

# DAG

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='refresh_v_c2025',
    default_args=default_args,
    start_date=datetime(2026, 2, 17),
    schedule='0 3,15 * * *',  # ежедневно в 03:00 и 15:00
    catchup=False,
    tags=['vitrina', 'contracts_2025', 'duckdb', 'data_quality'],
    max_active_runs=1,  # чтоб не накладывались запуски
    doc_md=__doc__
) as dag:

    refresh_task = PythonOperator(
        task_id='refresh_from_oracle_to_duckdb',
        python_callable=refresh_vitrina_from_oracle,
        provide_context=True,
        execution_timeout=timedelta(hours=2),  # таймаут 2 часа
    )

    # задача для логирования статистики

    def log_stats(**context):
        ti = context['ti']
        stats = ti.xcom_pull(task_ids='refresh_from_oracle_to_duckdb')
        print("СТАТИСТИКА КАЧЕСТВА ДАННЫХ:")
        print(f"   Всего записей: {stats['total_rows']:,}")
        print(f"   Чистые данные: {stats['clean_rows']:,} ({stats['clean_percent']}%)")
        print(f"   Приоритет 1: {stats['priority_1']:,}")
        print(f"   Приоритет 2: {stats['priority_2']:,}")
        
        # проверка порога качества

        if stats['clean_percent'] < 80:
            print("ВНИМАНИЕ: Качество данных ниже 80%!")
        
        return stats

    log_task = PythonOperator(
        task_id='log_quality_stats',
        python_callable=log_stats,
    )

    refresh_task >> log_task

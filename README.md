# Приветствую! Меня зовут Валерия :)

## _Data Engineer (Junior+/Middle) | Python • SQL • Airflow • Kafka_

# О себе:

Я занимаюсь анализом данных, автоматизацией и построением ETL/ELT-процессов.
Работала с большими таблицами (миллионы строк), аналитикой данных в сфере образования, туризма и гос. закупок, созданием отчетов, дашбордов и оптимизацией процессов.

# В моем стеке:

- **SQL** (Oracle, PostgreSQL, SQLite, MySQL, Greenplum, ClickHouse): витрины данных, оконные функции, партиционирование, индексы, написание пользовательских функций/хранимых процедур
- **DE**: Kafka, Docker, Oracle, PostgreSQL, Greenplum, Clickhouse, S3, ETL (Airflow, NiFi)
- **Python**: анализ данных с помощью pandas, numpy, визуализация с streamlit, подключение к различным бд
- **BI**: Power BI, Superset, DataLens
- **Диаграммы**: UML, BPMN, IDEF
- **Аналитика**: обработка, очистка данных, отчеты

Ищу возможности развиваться в аналитике и инженерии данных)

## 📁 Структура репозитория

### 🔹 SQL
| Проект | Кратко | Стек |
|--------|--------|------|
| `GP_udf_delta_upsert` | Инкрементальная загрузка в Greenplum через delta-upsert | Greenplum, PL/pgSQL |
| `GP_udf_delta_partition` | Загрузка по партициям с минимальными блокировками | Greenplum, External Tables |
| `GP_udf_mart` | Автоматическая сборка витрины `mart_store_day` | Greenplum, SQL Aggregations |
| `CH_shards_replicas` | Шаблон шардированных и реплицируемых таблиц | ClickHouse, ReplicatedMergeTree |
| `CH_dictionaries` | Словари для ускорения JOIN со справочниками | ClickHouse, Flat/Hashed Dicts |
| `ORA_proc_time` | Профайлер выполнения запросов в Oracle | Oracle PL/SQL, DBMS_UTILITY |

### 🔹 Python
| Проект | Кратко | Стек |
|--------|--------|------|
| `strlit_bi.py` | Self-service веб-интерфейс для фильтрации и выгрузки данных | Streamlit, Pandas, OpenPyXL |
| `tours_price_analysis.py` | EDA-анализ цен на туры с визуализацией | Pandas, Seaborn, Matplotlib |
| `parsing_script.py` | Парсер сайтов поставщиков с сохранением в CSV | Requests, BeautifulSoup4 |
| `analytics_script` | Pipeline очистки и дедупликации данных госзакупок (100К+ строк) | Pandas, thefuzz, Excel export |
| `kafka_consumer` | Consumer для CDC-событий из Kafka с загрузкой в PostgreSQL | confluent-kafka, PostgreSQL, UPSERT |

### 🔹 Airflow
| Проект | Кратко | Стек |
|--------|--------|------|
| `dag1.py` / `dag2.py` | ELT-пайплайн для витрины аналитики магазинов | Airflow, Greenplum, TaskGroup |
| `dag3.py` | Экспорт данных из DuckDB в S3-совместимое хранилище | Airflow, DuckDB, Yandex Object Storage |

### 🔹 Diagrams
| Проект | Кратко | Стек |
|--------|--------|------|
| `bpmn_to_be.png` | BPMN диаграмма, отражающая процесс формирования показателей финансовой устойчивости TO BE | BPMN |
| `uml_case.png` | UML диаграмма вариантов использования модуля автоматизации процесса расчета финансовой устойчивости | UML, Use Case |
| `uml_classes.jpg` | UML диаграмма классов, отражающая взаимодействие клиента, поставщика, кладовщика и бухгалтера | UML, Classes |
| `uml_sequence.jpg` | UML диаграмма последовательности, отражающая работу банкомата (снятие наличных) | UML, Sequence |

### 🔹 BI
| Проект | Кратко | Стек |
|--------|--------|------|
| `SS_dashboard1.jpg` | дашборд с витриной продаж, период янв-фев 2021 | Apache Superset |
| `SS_dashboard2.jpg` | дашборд с витриной продаж план-факт, период апрель 2021 | Apache Superset |

# Контакты

- Email: leogarechan@yandex.ru
- Telegram: @LeoGCh
- HH: https://hh.ru/resume/e9f438e1ff0f9d18860039ed1f677044523533

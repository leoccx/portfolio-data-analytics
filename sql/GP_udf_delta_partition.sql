-- создание udf (f_load_delta_partition) для загрузки в табл-факт по методу delta partition
-- учитываем также возможность загрузки новых данных на новые даты
-- диапазон загрузки <= 1 мес

CREATE OR REPLACE FUNCTION std14_135_fp.f_load_delta_partition(
    p_table text, -- имя целевой таблицы
	p_pxf_source text, -- имя внешней табл в PostgreSQL
	p_part_key text, -- ключ партиционирования
	p_start_date date, -- дата начала периода
	p_end_date date, -- дата конца периода
	p_user text, -- логин
	p_pass text -- пароль
)
RETURNS int4
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_ext_table text; -- имя внешней табл
	v_temp_table text; -- имя временной табл
	v_sql text; -- код sql
	v_pxf text; -- параметры pxf
	v_dist_clause text; -- для вытаскивания ключа дистрибуции
	v_oid oid; -- параметр oid
	v_cnt int8; -- для подсчета строк
	v_schema_name text; -- имя схемы
	v_table_name text; -- имя табл
	v_month_start date; -- начало мес
    v_month_end date; -- конец мес
	v_part_name text; -- имя партиции

BEGIN

-- разбиваем имя (чтобы в format название табл не воспринималось как единая строка)
v_schema_name := split_part(p_table, '.', 1); -- имя схемы
v_table_name := split_part(p_table, '.', 2); -- имя табл;

-- создаем имена внешней и временной табл
v_ext_table := v_table_name || '_ext';
v_temp_table := v_table_name || '_tmp_' || to_char(p_start_date, 'YYYYMM');

-- определяем мес загрузки
v_month_start := date_trunc('month', p_start_date)::date;
v_month_end := (date_trunc('month', p_start_date) + interval '1 month')::date;

-- создаем корректное имя партиции в соотв с мес загрузки
v_part_name := 'p' || to_char(v_month_start, 'YYYYMM');

-- проверяем, есть ли такая партиция в целевой табл (на случай загрузки новых данных на новые даты)
IF NOT EXISTS (
	SELECT 1
	FROM pg_partitions
	WHERE schemaname = v_schema_name
		AND tablename = v_table_name
		AND partitionrangestart = '''' || v_month_start || '''' || '::date'
		AND partitionrangeend = '''' || v_month_end || '''' || '::date'
)
THEN
	EXECUTE format(
		'ALTER TABLE %I.%I ADD PARTITION %I
		START (DATE %L) INCLUSIVE
		END (DATE %L) EXCLUSIVE',
        v_schema_name, v_table_name, v_part_name, v_month_start, v_month_end
    );
END IF;

-- получаем OID целевой табл
SELECT c.oid INTO v_oid
FROM pg_class c
JOIN pg_namespace n ON
	c.relnamespace = n.oid
WHERE
	n.nspname = v_schema_name
	AND c.relname = v_table_name;
    IF v_oid IS NOT NULL THEN
        v_dist_clause := pg_get_table_distributedby(v_oid); -- получаем ключ дистрибуции
	ELSE
        RAISE EXCEPTION 'Таблица % не найдена', p_table;
	END IF;

-- удаляем внешнюю табл, если есть
v_sql := format(
		'DROP EXTERNAL TABLE IF EXISTS %I.%I',
		v_schema_name, v_ext_table
	);

EXECUTE v_sql;

-- параметры для PXF
v_pxf := 'pxf://' || p_pxf_source ||
                 '?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres' ||
                 '&USER=' || p_user || '&PASS=' || p_pass;

-- создаем внешнюю табл
v_sql := format(-- исп для формирования единой строки
        'CREATE EXTERNAL TABLE %I.%I (LIKE %I.%I) 
		LOCATION (%L)
		FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
		ENCODING ''UTF8''',
        v_schema_name, v_ext_table, v_schema_name, v_table_name, v_pxf
    );

EXECUTE v_sql;

-- удаляем временную табл, если есть
v_sql := format(
		'DROP TABLE IF EXISTS %I.%I',
		v_schema_name, v_temp_table
	);

EXECUTE v_sql;

-- создаем временную табл
v_sql := format(
        'CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL) %s', -- including all - копируем все, включая констрейнты
v_schema_name, v_temp_table, v_schema_name, v_table_name, v_dist_clause
    );

EXECUTE v_sql;

-- вставляем данные за указанный период
v_sql := format(
        'INSERT INTO %I.%I SELECT * FROM %I.%I WHERE %I >= %L AND %I < %L',
        v_schema_name, v_temp_table, v_schema_name, v_ext_table, p_part_key, p_start_date, p_part_key, p_end_date
    );

EXECUTE v_sql;

-- получаем кол-во вставленных строк за последний sql-запрос
GET DIAGNOSTICS v_cnt = ROW_COUNT;

-- обмениваем партицию
v_sql := format(
        'ALTER TABLE %I.%I EXCHANGE PARTITION FOR (DATE %L) WITH TABLE %I.%I',
        v_schema_name, v_table_name, v_month_start, v_schema_name, v_temp_table
    );

EXECUTE v_sql;

-- удаляем внешнюю и временную табл
EXECUTE format('DROP EXTERNAL TABLE IF EXISTS %I.%I', v_schema_name, v_ext_table);
EXECUTE format('DROP TABLE IF EXISTS %I.%I', v_schema_name, v_temp_table);

RETURN v_cnt;

EXCEPTION -- обработка исключений
WHEN OTHERS THEN -- удаляем временную и внеш табл при ошибке
    EXECUTE format('DROP EXTERNAL TABLE IF EXISTS %I.%I', v_schema_name, v_ext_table);
	EXECUTE format('DROP TABLE IF EXISTS %I.%I', v_schema_name, v_temp_table);

RAISE;
END;
$$
EXECUTE ON MASTER;

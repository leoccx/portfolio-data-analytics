-- f_table - целевая табл
-- f_table_ext - внешняя табл
-- f_part_key - ключ партиционирования

CREATE OR REPLACE FUNCTION std14_135.f_load_du(
    f_table text, -- целевая табл
    f_table_ext text, -- внешняя табл
    f_part_key text -- ключ партиционирования
)
RETURNS text
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_months text[];
    v_month text;
BEGIN
    -- получаем список месяцев
    EXECUTE 'SELECT array_agg(DISTINCT to_char(' || f_part_key || ', ''YYYYMM'')) FROM ' || f_table_ext
    INTO v_months;

    -- цикл по месяцам
    FOREACH v_month IN ARRAY v_months
    LOOP
        RAISE NOTICE 'Загрузка месяца %', v_month;

        -- удаляем данные за месяц
        EXECUTE 'DELETE FROM ' || f_table || ' WHERE to_char(' || f_part_key || ', ''YYYYMM'') = ''' || v_month || '''';

        -- вставляем данные за месяц
        EXECUTE 'INSERT INTO ' || f_table || ' SELECT * FROM ' || f_table_ext || ' WHERE to_char(' || f_part_key || ', ''YYYYMM'') = ''' || v_month || '''';
    END LOOP;

    -- собираем статистику
    EXECUTE 'ANALYZE ' || f_table;

    RETURN 'Загрузка завершена';
EXCEPTION WHEN OTHERS THEN
    RETURN 'Ошибка: ' || SQLERRM;
END;
$$
EXECUTE ON MASTER;

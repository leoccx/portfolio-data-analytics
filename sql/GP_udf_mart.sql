-- создание пустой витрины уровня магазин*день с базовыми показателями для составления отчета

DROP TABLE std14_135_fp.mart_store_day;

CREATE TABLE std14_135_fp.mart_store_day (
    plant bpchar(4) NOT NULL,
    calday date NOT NULL,
    calday_txt text NOT NULL,
    calmonth_txt text NOT NULL,
    turnover NUMERIC,
    coupon_discount NUMERIC,
    turnover_net NUMERIC,
    qty_sold int8,
    qty_cheques int8,
    traffic int8,
    qty_promo_items int8,
    promo_items_percent NUMERIC,
    avg_items_per_cheque NUMERIC,
    conversion_coeff NUMERIC,
    avg_cheque NUMERIC,
    avg_revenue_per_visitor NUMERIC
)
WITH (
    appendonly = TRUE,
    orientation = column,
    compresstype = zstd,
    compresslevel = 1
)
DISTRIBUTED BY (plant)
PARTITION BY RANGE (calday) ( -- дата отчета 01.01.2021-28.02.2021
	START (DATE '2021-01-01') INCLUSIVE
    END (DATE '2021-03-01') EXCLUSIVE
    EVERY (INTERVAL '1 month')
);

-- созданиe udf (f_upd_mart) для загрузки И обновления витрины
-- учитывается возможность поступления новых данных
-- загрузка новых данных осуществляется по методу delta partition
-- диапазон загрузки <= 1 мес

CREATE OR REPLACE FUNCTION std14_135_fp.f_upd_mart(
    p_mart text, -- имя витрины
    p_part_key text, -- ключ партиционирования
    p_start_date date, -- начало диапазона
    p_end_date date -- конец диапазона
)
RETURNS int4
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_sql text;
    v_cnt int8;
    v_month_start date;
    v_month_end date;
    v_part_name text;
    v_temp_table text;
    v_schema_name text;
    v_table_name text;
    v_dist_clause text;
    v_oid oid;
BEGIN
    -- имя витрины
    v_schema_name := split_part(p_mart, '.', 1);
    v_table_name := split_part(p_mart, '.', 2);

    -- имя временной табл
    v_temp_table := v_table_name || '_tmp_' || to_char(p_start_date, 'YYYYMM');

    -- период партиции витрины
    v_month_start := date_trunc('month', p_start_date)::date;
    v_month_end := (v_month_start + interval '1 month')::date;
    v_part_name := 'p' || to_char(v_month_start, 'YYYYMM');

    -- проверка существования партиции
    IF NOT EXISTS (
        SELECT 1 FROM pg_partitions
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
             v_schema_name, v_table_name, v_part_name,
             v_month_start, v_month_end
        );
    END IF;

    -- дистрибуция
    SELECT c.oid INTO v_oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = v_schema_name
		AND c.relname = v_table_name;

    v_dist_clause := pg_get_table_distributedby(v_oid); -- ключ дистрибуции

    -- удаление временной табл, если есть
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', v_schema_name, v_temp_table);

    -- создание временной табл
    EXECUTE format(
        'CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL) %s',
        v_schema_name, v_temp_table,
        v_schema_name, v_table_name,
        v_dist_clause
    );

	-- заполнение временной табл (заготовка через format)
	v_sql := format(
	    'INSERT INTO %I.%I ',
	    v_schema_name,
	    v_temp_table
	);

	-- sql код для сборки витрины
	v_sql := v_sql || $q$
        WITH -- для конечных джойнов сначала создадим подзапросы
        sales AS ( -- продажи
            SELECT bh.plant, -- магазин
                   bh.calday, -- день
                   SUM(bi.rpa_sat) AS turnover, -- оборот с ндс
                   SUM(bi.qty) AS qty_sold, -- продано штук (штуки)
                   COUNT(DISTINCT bh.billnum) AS qty_cheques -- кол-во чеков (кол-во уникальных за опр период времени)
            FROM std14_135_fp.bills_head1 bh
            JOIN std14_135_fp.bills_item1 bi -- берем ТОЛЬКО совпадения с обеих сторон - нужны ФАКТЫ ПРОДАЖ 
				ON bh.billnum = bi.billnum -- джойн по номеру чека
            WHERE bh.calday >= $1
				AND bh.calday < $2
            GROUP BY bh.plant, bh.calday -- группировка по магазинам и дню (магазин*день)
        ),
        discounts AS ( -- акции (отдельно от sales, т.к. не во всех чеках есть купоны + проще просто разделить)
            SELECT
                bh.plant, -- магазин
                bh.calday, -- день
				SUM(c.discount) AS coupon_discount, -- скидка по купону
                COUNT(c.plant) AS qty_promo_items -- кол-во товаров по акции
            FROM std14_135_fp.bills_head1 bh
            JOIN std14_135_fp.bills_item1 bi 
				ON bh.billnum = bi.billnum -- джойн по номеру чека
			-- джойн с купонами, т.к. именно там указано, какая акция, в каком чеке и к какому товару
            JOIN std14_135_fp.coupons1 c  
			-- 2 поля, т.к. купон привязан к КОНКРЕТНОМУ ТОВАРУ в ЧЕКЕ
				ON c.cheque = bh.billnum -- по номеру чека
				AND c.product = bi.material -- и по товару
            WHERE bh.calday >= $1
				AND bh.calday < $2
            GROUP BY bh.plant, bh.calday -- группировка магазин*день
        ),
        traf AS ( -- трафик
            SELECT 
				plant, -- магазин
                "date" AS calday, -- день
                SUM(quantity) AS traffic -- сумма трафика
            FROM std14_135_fp.traffic1
            WHERE "date" >= $1
				AND "date" < $2
            GROUP BY plant, "date" -- группировка магазин*день
        ),
        agg AS ( -- склейка sales (ГЛАВ ТАБЛ), discounts, traf с обработкой возможных null значений
            SELECT
            	COALESCE(s.plant, t.plant) AS plant, -- магазин
				COALESCE(s.calday, t.calday) AS calday, -- день
				s.turnover, -- оборот
				COALESCE(d.coupon_discount, 0) AS coupon_discount, -- скидок может НЕ быть для какого-то дня = NULL
				-- пустые значения в coupon_discount заменим на 0 для удобочитаемости и во избежание ошибок
				d.qty_promo_items, -- кол-во товаров по акции
				s.qty_sold, -- продано штук (тоже NULL не мб, т.к. sales - глав табл, обязана быть)
				s.qty_cheques, -- кол-во чеков (тоже NULL не мб)
                t.traffic AS traffic -- если входов в магазин не было, то...
				-- ...мб NULL или вообще не быть значений
            FROM sales s
            FULL JOIN traf t
			-- здесь фулл джойн, т.к. в эксель трафик считается и за вообще все дни       
				ON t.plant = s.plant 
				AND t.calday = s.calday
			LEFT JOIN discounts d 
			-- left join от sales, т.к. это приоритетный кусок: 
			-- если нет продаж, то остальные данные не нужны
			-- можно было бы основываться на датах и учитывать ситуацию, когда продаж нет
			-- в реальных, НЕучебных условиях, на мой взгляд, было бы корректнее основываться на датах
			-- тогда нужно было бы в кач-ве глав табл делать условный календарь с датами и от него уже left join
				ON d.plant = COALESCE(s.plant, t.plant) -- делаем coalesce из-за full join ранее
       			AND d.calday = COALESCE(s.calday, t.calday)
        )
        SELECT
            plant, -- завод (магазин) (текст подтяну в суперсете),
			calday, -- день (дата)
            to_char(calday, 'YYYY-MM-DD') AS calday_txt, -- день (текст)
			to_char(calday, 'YYYY-MM') AS calmonth_txt, -- месяц (текст)
            turnover, -- оборот
            coupon_discount, -- скидка по купону
            turnover - coupon_discount AS turnover_net, -- оборот с учетом скидки
            qty_sold, -- кол-во проданных товаров
            qty_cheques, -- кол-во чеков
            traffic, -- трафик
            qty_promo_items, -- кол-во товаров по акции
            (qty_promo_items::NUMERIC / qty_sold::NUMERIC) AS promo_items_percent, -- доля товаров со скидкой
			-- приводим к типу numeric для точного деления
    		(qty_sold::NUMERIC / qty_cheques::NUMERIC) AS avg_items_per_cheque, -- ср кол-во товаров в чеке
            (100.0 * qty_cheques::NUMERIC / traffic::NUMERIC) AS conversion_coeff, -- коэф конверсии
    		(turnover::NUMERIC / qty_cheques::NUMERIC) AS avg_cheque, -- ср чек
    		(turnover::NUMERIC / traffic::NUMERIC) AS avg_revenue_per_visitor -- ср выручка на посетителя
        FROM agg;
    $q$;

    EXECUTE v_sql -- заполняем временную табл данными
		USING p_start_date, p_end_date; -- передаем параметры на места $1 и $2

    GET DIAGNOSTICS v_cnt = ROW_COUNT;

    -- подмена партиции
    EXECUTE format(
        'ALTER TABLE %I.%I EXCHANGE PARTITION FOR (DATE %L) WITH TABLE %I.%I',
        v_schema_name, v_table_name, v_month_start, v_schema_name, v_temp_table
    );

	-- удаление временной табл
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', v_schema_name, v_temp_table);

    RETURN v_cnt;

	EXCEPTION -- обработка исключений
	WHEN OTHERS THEN -- удаляем временную табл при ошибке
		EXECUTE format('DROP TABLE IF EXISTS %I.%I', v_schema_name, v_temp_table);
	
	RAISE;
END;

$$
EXECUTE ON MASTER;

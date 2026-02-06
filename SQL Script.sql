-- таблица купоны (справочник)
CREATE TABLE IF NOT EXISTS std15_35.coupons(plant bpchar(4), date date, coupon_id bpchar(10), promotion_id bpchar(50), item_id int8, check_id int8)
WITH (
	appendonly = true,
	orientation = column,
	compresstype = zstd,
	compresslevel = 1
)
DISTRIBUTED REPLICATED

-- таблица промоакции (справочник)
CREATE TABLE IF NOT EXISTS std15_35.promos(promotion_id bpchar(50), promo_name bpchar(100), promo_type bpchar(3), item_id int8, discount_size int4)
WITH (
	appendonly = true,
	orientation = column,
	compresstype = zstd,
	compresslevel = 1
)
DISTRIBUTED REPLICATED

-- таблица типы промоакций (справочник)
CREATE TABLE IF NOT EXISTS std15_35.promo_types(promo_type bpchar(3), promo_text text)
WITH (
	appendonly = true,
	orientation = column,
	compresstype = zstd,
	compresslevel = 1
)
DISTRIBUTED REPLICATED

-- таблица с чеками
CREATE TABLE std15_35.bills_head (
	billnum int8 NULL,
	plant bpchar(4) NULL,
	calday date NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (billnum)
PARTITION BY RANGE(calday) 
          (
          START ('2020-01-01'::date) END ('2022-01-01'::date) EVERY ('1 mon'::interval), 
          DEFAULT PARTITION other 
);


-- таблица с чеками 2
CREATE TABLE std15_35.bills_item (
	billnum int8 NULL,
	billitem int8 NULL,
	material int8 NULL,
	qty int8 NULL,
	netval numeric(17, 2) NULL,
	tax numeric(17, 2) NULL,
	rpa_sat numeric(17, 2) NULL,
	calday date NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (billnum, billitem)
PARTITION BY RANGE(calday) 
          (
          START ('2020-01-01'::date) END ('2022-01-01'::date) EVERY ('1 mon'::interval),
          DEFAULT PARTITION other
);


-- таблица с траффиком
CREATE TABLE std15_35.traffic (
	plant bpchar(4) NULL,
	"date" date NULL,
	"time" bpchar(6) NULL,
	frame_id bpchar(10) NULL,
	quantity int4 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(date) 
          (
          START ('2021-01-01'::date) END ('2022-01-01'::date) EVERY ('1 mon'::interval),
          DEFAULT PARTITION other  WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1') 
);


-- таблица магазинов (справочник)
CREATE TABLE std15_35.stores (
	str_id varchar(10) NULL,
	str_name text NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

-- функция для загрузки данных для справочников (FULL, метод TRUNCATE + INSERT)
CREATE OR REPLACE FUNCTION std15_35.f_load_full(p_table text, p_file_name text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
v_ext_table_name text;
v_sql text;
v_gpfdist text;
v_result int4;

BEGIN
	v_ext_table_name = p_table||'_ext';
	
	EXECUTE 'TRUNCATE TABLE '||p_table;
	
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_name;
	
	v_gpfdist = 'gpfdist://172.16.128.226:8080/'||p_file_name||'.csv';
	
	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_name||'(LIKE '||p_table||')
			LOCATION ('''||v_gpfdist||''')
			ON ALL
			FORMAT ''CSV'' (HEADER DELIMITER '';''	NULL '''' ESCAPE ''"'' QUOTE ''"'' )
			ENCODING ''UTF8''';
	RAISE NOTICE 'EXTERNAL TABLE IS : %', v_sql;
	
	EXECUTE v_sql;
	
	EXECUTE 'INSERT INTO '||p_table||' SELECT * FROM '||v_ext_table_name;
	
	EXECUTE 'SELECT COUNT(1) FROM '||p_table INTO v_result;
	
	RETURN v_result;
END

$$
EXECUTE ON ANY;

-- функция вырезка новых партиций из дефолтной
create or replace function std15_35.f_create_date_partitions(p_table_name text, p_partition_value date)
-- p_table_name - это таблица, для которой делаем вырезку, p_partition_value - это дата, до которой мы хотим сделать партиции
	returns void 
	language plpgsql
	volatile
as $$

declare 
	v_cnt_partitions int;
	v_table_name text;
	v_partition_end_sql text;
	v_partition_end date;
	v_interval interval;
	v_ts_format text:='YYYY-MM-DD';
	v_split text;
begin
	v_table_name = lower(trim(translate(p_table_name, ';/''', '')));

	--Проверка наличия партиций у таблицы
	select count(*) into v_cnt_partitions
	from pg_partitions p
	where p.schemaname||'.'||p.tablename = v_table_name;
	
	if v_cnt_partitions > 1 then
		loop 
			--Получение параметров последней партиции
			select partitionrangeend into v_partition_end_sql
				from ( 
					select p.*, rank() over (order by partitionrank desc) rnk 
					from pg_partitions p
					where p.partitionrank is not null and schemaname||'.'||tablename = v_table_name 
					) q
				where rnk = 1;
			
			--конечная дата последней партиции
			execute 'select '||v_partition_end_sql into v_partition_end;
		
			--если партиция уже есть для входного значения, тогда exit из функции
			exit when v_partition_end > p_partition_value;
			
			v_interval := '1 month'::interval;
		
		
			--вырез новой партиции из дефолтной партиции, если её ещё не существует
			v_split = 'alter table '||v_table_name||' split default partition
					 start ('||v_partition_end_sql||') end ('''||to_char(v_partition_end+v_interval, v_ts_format)||'''::date)';
		
			raise notice '----%', v_split;
			execute v_split;
		end loop;
	end if;
end;
$$
execute on any;


-- функция для загрузки данных через подмену партиции (delta partition)
create or replace function std15_35.f_load_delta_partitions(p_table_from_name text, p_table_to_name text, p_partition_key text,
											   			  p_start_date date, p_end_date date, p_user_id text, p_pass text)
											   
	returns int8 
	language plpgsql
	volatile
as $$

declare 
	v_load_interval interval;
	v_iterDate date;
	v_where text;
	v_temp_table text;
	v_cnt_prt int8;
	v_cnt int8;
	v_start_date date;
	v_end_date date;

	v_ext_table text;
	v_table_oid int4;
	v_params text;
	v_dist_key text;

	v_sql text;
	v_pxf text;
	
	
	
	
begin 
	
	v_cnt = 0;
	perform std15_35.f_create_date_partitions(p_table_to_name, p_end_date);
	

	v_ext_table = p_table_to_name||'_ext';
	
	
	select c.oid               -- получаем id целевой таблицы чтобы получить по нему ключ дистрибуции
	into v_table_oid
	from pg_class as c inner join pg_namespace as n on c.relnamespace = n.oid
	where n.nspname||'.'||c.relname = p_table_to_name
	limit 1;

	if v_table_oid = 0 or v_table_oid is null then 
		v_dist_key = 'DISTRIBUTED RANDOMLY';
	else
		v_dist_key = pg_get_table_distributedby(v_table_oid);    --получаем дистрибуцию
	end if;

	select coalesce('with ('||array_to_string(reloptions,', ')||')','')    --получаем параметры целевой таблицы
	from pg_class
	into v_params
	where oid = p_table_to_name::regclass;

	execute 'drop external table if exists '||v_ext_table;


	v_pxf = 'pxf://'||p_table_from_name||'?PROFILE=jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='||p_user_id||'&PASS='||p_pass;

	raise notice '>PXF connection query is: %', v_pxf;


	if p_table_from_name = 'gp.traffic' then
		v_sql = 'create external table '||v_ext_table||'
					(plant bpchar(4),
					"date" bpchar(10) ,
					"time" bpchar(6) ,
					frame_id bpchar(10) ,
					quantity int4 )
				 location ('''||v_pxf||'''
				 ) on all
				 format ''custom'' (formatter=''pxfwritable_import'')
				 encoding ''utf8''';
	else
		v_sql = 'create external table '||v_ext_table||'(like '||p_table_to_name||')
				 location ('''||v_pxf||'''
				 ) on all
				 format ''custom'' (formatter=''pxfwritable_import'')
				 encoding ''utf8''';
	end if;
	raise notice '>Query for external table is: %', v_sql;

	execute v_sql;

	v_load_interval = '1 month'::interval;
	v_start_date := date_trunc('month', p_start_date)::date;
	v_end_date := date_trunc('month', p_end_date)::date + v_load_interval;

	loop 
		
		v_iterDate = v_start_date + v_load_interval;
		
		exit when (v_iterDate > v_end_date);
	
		v_temp_table = p_table_to_name||'_prt';
		v_sql = 'create table '||v_temp_table||'(like '||p_table_to_name||') '||v_params||' '||v_dist_key||';';
		raise notice '>Query for temp table is: %', v_sql;
		execute v_sql;
	
		if p_table_from_name = 'gp.traffic' then
			v_where = 'TO_DATE('||p_partition_key||', ''DD/MM/YYYY'')>='''||v_start_date||'''::date and TO_DATE('||p_partition_key||', ''DD/MM/YYYY'')<'''||v_iterDate||'''::date';
			
			v_sql = 'insert into '||v_temp_table||' 
					 select plant, TO_DATE('||p_partition_key||', ''DD/MM/YYYY''), "time", frame_id, quantity 
					 from '||v_ext_table||' where '||v_where;
		else 
			v_where = p_partition_key||'>='''||v_start_date||'''::date and '||p_partition_key||'<'''||v_iterDate||'''::date';
			v_sql = 'insert into '||v_temp_table||' select * from '||v_ext_table||' where '||v_where;
		end if;
	
		
		execute v_sql;
	
	
		get diagnostics v_cnt_prt = row_count;
		v_cnt = v_cnt + v_cnt_prt;
		
		v_sql = 'alter table '||p_table_to_name||' exchange partition for (date '''||v_start_date||''') with table '||v_temp_table||' with validation';
		raise notice '>Query for exchange partition: %', v_sql;
		execute v_sql;
	
		execute 'drop table '||v_temp_table;
	
		v_start_date := v_iterDate;

	end loop;
	
	return v_cnt;

end;

$$
execute on any;

-- Расчет всех штук для чеков
SELECT bh.plant, SUM(bi.rpa_sat) AS "Оборот", SUM(bi.qty) AS "Кол-во проданных товаров", COUNT(DISTINCT bi.billnum) AS "Количество чеков"
FROM std15_35.bills_item bi
JOIN std15_35.bills_head bh ON bi.billnum = bh.billnum
WHERE bi.calday between '2021-01-01' AND '2021-02-28'
GROUP BY bh.plant
ORDER BY bh.plant


-- Расчет скидки по купонам (почти правильно)
SELECT c.plant,
SUM(CASE 
	WHEN p.promo_type = '001' THEN 
	p.discount_size 
	WHEN p.promo_type = '002' THEN 
	(bi.rpa_sat / bi.qty) * p.discount_size / 100.0
END
) AS "Расчет скидки по купонам"
FROM std15_35.coupons c
JOIN std15_35.bills_item bi 
    ON c.material = bi.material 
   AND c.billnum  = bi.billnum
JOIN std15_35.promo p 
    ON c.promotion_id = p.promotion_id
GROUP BY c.plant
ORDER BY c.plant;


-- Кол-во товаров по акции
SELECT COUNT(c.material) AS "Кол-во товаров по акции"
FROM std15_35.coupons c
GROUP BY c.plant
ORDER BY c.plant


-- CTE Расчет скидки по купонам
WITH cte_coupons AS (
	SELECT c.plant,
	SUM(CASE 
		WHEN p.promo_type = '001' THEN 
		p.discount_size 
		WHEN p.promo_type = '002' THEN 
		(bi.rpa_sat / bi.qty) * p.discount_size / 100.0
	END
	) AS "Расчет скидки по купонам"
	FROM std15_35.coupons c
	JOIN std15_35.bills_item bi 
	    ON c.material = bi.material 
	   AND c.billnum  = bi.billnum
	JOIN std15_35.promo p 
	    ON c.promotion_id = p.promotion_id
	GROUP BY c.plant
	ORDER BY c.plant
),
-- CTE Расчет всех штук для чеков
cte_bills AS (
	SELECT bh.plant, SUM(bi.rpa_sat) as "Оборот", SUM(bi.qty) AS "Кол-во проданных товаров", COUNT(DISTINCT bi.billnum) AS "Количество чеков"
	FROM std15_35.bills_item bi
	JOIN std15_35.bills_head bh ON bi.billnum = bh.billnum
	WHERE bi.calday between '2021-01-01' AND '2021-02-28'
	GROUP BY bh.plant
	ORDER BY bh.plant
),
-- CTE Расчета трафика
cte_traffic AS (
	SELECT SUM(t.quantity) AS "Трафик", t.plant
	FROM std15_35.traffic t
	GROUP BY t.plant
	ORDER BY t.plant
)
-- Расчет с помощью CTE (Оборот с учетом скидки)
SELECT cb.plant, cb."Оборот" - cc."Расчет скидки по купонам" AS "Оборот с учетом скидки", ct."Трафик"
FROM cte_bills cb
JOIN cte_coupons cc ON cb.plant = cc.plant
JOIN cte_traffic ct ON cb.plant = ct.plant
ORDER BY cb.plant













-- Расчет Средняя выручка на одного посетителя, руб с помощью CTE
WITH cte_coupons AS (
	SELECT c.plant,
	CASE
		WHEN p.promo_type = '001' THEN
		SUM(bi.rpa_sat * (1 - (p.discount_size / 100)))
		WHEN p.promo_type = '002' THEN
		SUM(bi.rpa_sat - p.discount_size)
		END AS "Скидки по купонам"
	FROM std15_35.coupons c
	JOIN std15_35.promo p ON c.promotion_id = p.promotion_id
	JOIN std15_35.bills_item bi ON c.billnum = bi.billnum AND c.material = bi.material
	GROUP BY c.plant, p.promo_type
),
cte_bills AS (
	SELECT bh.plant, SUM(bi.rpa_sat) as "Оборот с НДС", SUM(bi.qty) AS "Кол-во проданных товаров", COUNT(DISTINCT bi.billnum) AS "Количество чеков"
	FROM std15_35.bills_item bi
	JOIN std15_35.bills_head bh ON bi.billnum = bh.billnum
	WHERE bi.calday between '2021-01-01' AND '2021-02-28'
	GROUP BY bh.plant
	ORDER BY bh.plant
),
-- CTE Расчета трафика
cte_traffic AS (
	SELECT t.plant, s.text, SUM(t.quantity) AS "Трафик"
	FROM std15_35.traffic t
	JOIN std15_35.stores s
	GROUP BY t.plant
	ORDER BY t.plant
)
SELECT ct.plant
,
--cb."Оборот с НДС" - cc."Скидки по купонам" AS "Оборот с учетом скидки",
ROUND(CASE
	WHEN ct."Трафик" = 0 THEN 0
	ELSE cb."Оборот с НДС" / ct."Трафик"
	END, 1) AS "Средняя выручка на одного посетителя, руб"
FROM cte_traffic ct
JOIN cte_bills cb ON cb.plant = ct.plant
ORDER BY ct.plant



-- Основной код (Готовая витрина данных)
WITH cte_bills AS (
	SELECT bh.plant, SUM(bi.rpa_sat) AS "Оборот", SUM(bi.qty) AS "Кол-во проданных товаров", COUNT(DISTINCT bi.billnum) AS "Количество чеков"
	FROM std15_35.bills_item bi
	JOIN std15_35.bills_head bh ON bi.billnum = bh.billnum
	WHERE bi.calday between '2021-01-01' AND '2021-02-28'
	GROUP BY bh.plant
	ORDER BY bh.plant
),
cte_coupons AS (
	SELECT c.plant,
	SUM(CASE 
		WHEN p.promo_type = '001' THEN 
		p.discount_size 
		WHEN p.promo_type = '002' THEN 
		(bi.rpa_sat / bi.qty) * p.discount_size / 100.0
	END
	) AS "Скидки по купонам", COUNT(c.material) AS "Кол-во товаров по акции"
	FROM std15_35.coupons c
	JOIN std15_35.bills_item bi 
	    ON c.material = bi.material 
	   AND c.billnum  = bi.billnum
	JOIN std15_35.promo p 
	    ON c.promotion_id = p.promotion_id
	GROUP BY c.plant
	ORDER BY c.plant
),
cte_traffic AS (
	SELECT t.plant, s.plant_name, SUM(t.quantity) AS "Трафик"
	FROM std15_35.traffic t
	JOIN std15_35.stores s ON t.plant = s.plant
	GROUP BY t.plant, s.plant_name
	ORDER BY t.plant
)
SELECT 
	ct.plant 									AS "Завод", 
	ct.plant_name 								AS "Завод (текст)", 
	cb."Оборот", 
	cc."Скидки по купонам", 
	cb."Оборот" - cc."Скидки по купонам" 		AS "Оборот с учетом скидки",
	cb."Кол-во проданных товаров",
	cb."Количество чеков",
	ct."Трафик",
	cc."Кол-во товаров по акции",
	ROUND((cc."Кол-во товаров по акции" 
	/ cb."Кол-во проданных товаров") * 100, 1) 	AS "Доля товаров со скидкой",
	ROUND(cb."Кол-во проданных товаров" 
	/ cb."Количество чеков", 2) 				AS "Среднее количество товаров в чеке",
	ROUND(((cb."Количество чеков"::float 
	/ ct."Трафик") * 100)::numeric, 2) 			AS "Коэффициент конверсии магазина, %",
	ROUND(cb."Оборот" /
	cb."Количество чеков", 1) 					AS "Средний чек, руб ",
	ROUND(CASE
		WHEN ct."Трафик" = 0 THEN 0
		ELSE cb."Оборот" / ct."Трафик"
	END, 1) 									AS "Средняя выручка на одного посетителя, руб "
FROM cte_traffic ct
JOIN cte_coupons cc ON ct.plant = cc.plant
JOIN cte_bills cb ON ct.plant = cb.plant
ORDER BY ct.plant


-- Оптимизированный код (1. Оптимизация CTE cte_coupons, 2. Убрали лишние ORDER BY, 3. Привели явно к одному типу в ROUND (в select))
EXPLAIN ANALYZE WITH cte_bills AS (
	SELECT bh.plant, SUM(bi.rpa_sat) AS "Оборот", SUM(bi.qty) AS "Кол-во проданных товаров", COUNT(DISTINCT bi.billnum) AS "Количество чеков"
	FROM std15_35.bills_item bi
	JOIN std15_35.bills_head bh ON bi.billnum = bh.billnum
	WHERE bi.calday between '2021-01-01' AND '2021-02-28'
	AND bh.calday BETWEEN '2021-01-01' AND '2021-02-28'
	GROUP BY bh.plant
),
cte_coupons AS (
	SELECT c.plant,
	SUM(CASE 
		WHEN p.promo_type = '001' THEN 
		p.discount_size 
		WHEN p.promo_type = '002' THEN 
		(bi.rpa_sat / bi.qty) * p.discount_size / 100.0
	END
	) AS "Скидки по купонам", COUNT(c.material) AS "Кол-во товаров по акции"
	FROM std15_35.coupons c
	JOIN std15_35.bills_item bi 
	    ON c.material = bi.material 
	   AND c.billnum  = bi.billnum
	   AND bi.calday BETWEEN '2021-01-01' AND '2021-02-28' -- Нужно для того, чтобы сократить сканирование лишних данных
	JOIN std15_35.promo p 
	    ON c.promotion_id = p.promotion_id
	GROUP BY c.plant
),
cte_traffic AS (
	SELECT t.plant, s.plant_name, SUM(t.quantity) AS "Трафик"
	FROM std15_35.traffic t
	JOIN std15_35.stores s ON t.plant = s.plant
	GROUP BY t.plant, s.plant_name
)
SELECT 
	ct.plant 											AS "Завод", 
	ct.plant_name 										AS "Завод (текст)", 
	cb."Оборот", 
	cc."Скидки по купонам", 
	cb."Оборот" - cc."Скидки по купонам" 				AS "Оборот с учетом скидки",
	cb."Кол-во проданных товаров",
	cb."Количество чеков",
	ct."Трафик",
	cc."Кол-во товаров по акции",
	ROUND((cc."Кол-во товаров по акции"::numeric 
	/ cb."Кол-во проданных товаров"::numeric) * 100, 1)	AS "Доля товаров со скидкой",
	ROUND(cb."Кол-во проданных товаров"::numeric 
	/ cb."Количество чеков"::numeric, 2) 				AS "Среднее количество товаров в чеке",
	ROUND(((cb."Количество чеков"::numeric 
	/ ct."Трафик") * 100)::numeric, 2) 					AS "Коэффициент конверсии магазина, %",
	ROUND(cb."Оборот"::numeric /
	cb."Количество чеков"::numeric, 1) 					AS "Средний чек, руб ",
	ROUND(CASE
		WHEN ct."Трафик" = 0 THEN 0
		ELSE cb."Оборот" / ct."Трафик"
	END, 1) 											AS "Средняя выручка на одного посетителя, руб "
FROM cte_traffic ct
JOIN cte_coupons cc ON ct.plant = cc.plant
JOIN cte_bills cb ON ct.plant = cb.plant
ORDER BY ct.plant



-- VIEW (Представление)
CREATE OR REPLACE VIEW std15_35.datamart AS (WITH cte_bills AS (
	SELECT bh.plant, SUM(bi.rpa_sat) AS "Оборот", SUM(bi.qty) AS "Кол-во проданных товаров", COUNT(DISTINCT bi.billnum) AS "Количество чеков"
	FROM std15_35.bills_item bi
	JOIN std15_35.bills_head bh ON bi.billnum = bh.billnum
	WHERE bi.calday between '2021-01-01' AND '2021-02-28'
	AND bh.calday BETWEEN '2021-01-01' AND '2021-02-28'
	GROUP BY bh.plant
),
cte_coupons AS (
	SELECT c.plant,
	SUM(CASE 
		WHEN p.promo_type = '001' THEN 
		p.discount_size 
		WHEN p.promo_type = '002' THEN 
		(bi.rpa_sat / bi.qty) * p.discount_size / 100.0
	END
	) AS "Скидки по купонам", COUNT(c.material) AS "Кол-во товаров по акции"
	FROM std15_35.coupons c
	JOIN std15_35.bills_item bi 
	    ON c.material = bi.material 
	   AND c.billnum  = bi.billnum
	   AND bi.calday BETWEEN '2021-01-01' AND '2021-02-28' -- Нужно для того, чтобы сократить сканирование лишних данных
	JOIN std15_35.promo p 
	    ON c.promotion_id = p.promotion_id
	GROUP BY c.plant
),
cte_traffic AS (
	SELECT t.plant, s.plant_name, SUM(t.quantity) AS "Трафик"
	FROM std15_35.traffic t
	JOIN std15_35.stores s ON t.plant = s.plant
	GROUP BY t.plant, s.plant_name
)
SELECT 
	ct.plant 											AS "Завод", 
	ct.plant_name 										AS "Завод (текст)", 
	cb."Оборот", 
	cc."Скидки по купонам", 
	cb."Оборот" - cc."Скидки по купонам" 				AS "Оборот с учетом скидки",
	cb."Кол-во проданных товаров",
	cb."Количество чеков",
	ct."Трафик",
	cc."Кол-во товаров по акции",
	ROUND((cc."Кол-во товаров по акции"::numeric 
	/ cb."Кол-во проданных товаров"::numeric) * 100, 1)	AS "Доля товаров со скидкой",
	ROUND(cb."Кол-во проданных товаров"::numeric 
	/ cb."Количество чеков"::numeric, 2) 				AS "Среднее количество товаров в чеке",
	ROUND(((cb."Количество чеков"::numeric 
	/ ct."Трафик") * 100)::numeric, 2) 					AS "Коэффициент конверсии магазина, %",
	ROUND(cb."Оборот"::numeric /
	cb."Количество чеков"::numeric, 1) 					AS "Средний чек, руб ",
	ROUND(CASE
		WHEN ct."Трафик" = 0 THEN 0
		ELSE cb."Оборот" / ct."Трафик"
	END, 1) 											AS "Средняя выручка на одного посетителя, руб "
FROM cte_traffic ct
JOIN cte_coupons cc ON ct.plant = cc.plant
JOIN cte_bills cb ON ct.plant = cb.plant
ORDER BY ct.plant
);


-- Функция
CREATE OR REPLACE FUNCTION std15_35.f_report_dm(
    p_start_date text,
    p_end_date   text
)
RETURNS int4
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_sql        text;
    v_start_key  text;
    v_end_key    text;
    v_view_name  text;
BEGIN
    -- 1. Чистим даты для имени VIEW
    v_start_key := TRIM(TRANSLATE(p_start_date, '-', ''));
    v_end_key   := TRIM(TRANSLATE(p_end_date,   '-', ''));

    v_view_name := 'datamart_' || v_start_key || '_' || v_end_key;

    -- 2. Формируем SQL
    v_sql := format($fmt$
        CREATE OR REPLACE VIEW std15_35.%I AS
        WITH cte_bills AS (
            SELECT
                bh.plant,
                SUM(bi.rpa_sat) AS "Оборот",
                SUM(bi.qty) AS "Кол-во проданных товаров",
                COUNT(DISTINCT bi.billnum) AS "Количество чеков"
            FROM std15_35.bills_item bi
            JOIN std15_35.bills_head bh
              ON bi.billnum = bh.billnum
            WHERE bi.calday BETWEEN %L AND %L
              AND bh.calday BETWEEN %L AND %L
            GROUP BY bh.plant
        ),
        cte_coupons AS (
            SELECT
                c.plant,
                SUM(
                    CASE 
                        WHEN p.promo_type = '001' THEN 
                            p.discount_size 
                        WHEN p.promo_type = '002' THEN 
                            (bi.rpa_sat / bi.qty) * p.discount_size / 100.0
                    END
                ) AS "Скидки по купонам",
                COUNT(c.material) AS "Кол-во товаров по акции"
            FROM std15_35.coupons c
            JOIN std15_35.bills_item bi 
              ON c.material = bi.material 
             AND c.billnum  = bi.billnum
             AND bi.calday BETWEEN %L AND %L
            JOIN std15_35.promo p 
              ON c.promotion_id = p.promotion_id
			WHERE c.date BETWEEN %L AND %L
            GROUP BY c.plant
        ),
        cte_traffic AS (
            SELECT
                t.plant,
                s.plant_name,
                SUM(t.quantity) AS "Трафик"
            FROM std15_35.traffic t
            JOIN std15_35.stores s
              ON t.plant = s.plant
			WHERE t.date BETWEEN %L AND %L
            GROUP BY t.plant, s.plant_name
        )
        SELECT 
            ct.plant                                         AS "Завод", 
            ct.plant_name                                    AS "Завод (текст)", 
            cb."Оборот", 
            cc."Скидки по купонам", 
            cb."Оборот" - cc."Скидки по купонам"             AS "Оборот с учетом скидки",
            cb."Кол-во проданных товаров",
            cb."Количество чеков",
            ct."Трафик",
            cc."Кол-во товаров по акции",

            ROUND(
                (cc."Кол-во товаров по акции"::numeric 
                / cb."Кол-во проданных товаров"::numeric) * 100, 
                1
            ) AS "Доля товаров со скидкой",

            ROUND(
                cb."Кол-во проданных товаров"::numeric 
                / cb."Количество чеков"::numeric, 
                2
            ) AS "Среднее количество товаров в чеке",

            ROUND(
                ((cb."Количество чеков"::numeric 
                / ct."Трафик") * 100)::numeric, 
                2
            ) AS "Коэффициент конверсии магазина, %%",

            ROUND(
                cb."Оборот"::numeric /
                cb."Количество чеков"::numeric, 
                1
            ) AS "Средний чек, руб ",

            ROUND(
                CASE
                    WHEN ct."Трафик" = 0 THEN 0
                    ELSE cb."Оборот" / ct."Трафик"
                END, 
                1
            ) AS "Средняя выручка на одного посетителя, руб "
        FROM cte_traffic ct
        JOIN cte_coupons cc ON ct.plant = cc.plant
        JOIN cte_bills   cb ON ct.plant = cb.plant
        ORDER BY ct.plant;
    $fmt$,
        v_view_name,
        p_start_date, p_end_date,
		p_start_date, p_end_date,
		p_start_date, p_end_date,
        p_start_date, p_end_date,
        p_start_date, p_end_date
    );

    -- 3. Для отладки
    RAISE NOTICE 'SQL: %', v_sql;

    -- 4. Выполняем SQL
    EXECUTE v_sql;

    RETURN 1;
END;
$$;







-- Создание таблицы в ClickHouse
CREATE TABLE std15_35.datamart_finalproject_ch
(
    `Завод` String,
    `Завод (Текст)` String,
    `Оборот` Int64,
    `Скидки по купонам` Float64,
    `Оборот с учетом скидки` Float64,
    `Кол-во проданных товаров` Int64,
    `Количество чеков` Int64,
    `Трафик` Int64,
    `Кол-во товаров по акции` Int64,
    `Доля товаров со скидкой, %` Float64,
    `Среднее количество товаров в чеке` Float64,
    `Коэффициент конверсии магазина, %` Float64,
    `Средний чек, руб` Float64,
    `Сред. выручка на посетителя, руб` Float64
)
ENGINE = MergeTree
ORDER BY (`Завод`);

-- Загрузка данных из представления в Greenplum с приведением типов
INSERT INTO std15_35.datamart_finalproject_ch
SELECT
    "Завод",
    "Завод (текст)",
    toInt64("Оборот") AS "Оборот",
    toFloat64("Скидки по купонам") AS "Скидки по купонам",
    toFloat64("Оборот с учетом скидки") AS "Оборот с учетом скидки",
    toInt64("Кол-во проданных товаров") AS "Кол-во проданных товаров",
    toInt64("Количество чеков") AS "Количество чеков",
    toInt64("Трафик") AS "Трафик",
    toInt64("Кол-во товаров по акции") AS "Кол-во товаров по акции",
    toFloat64("Доля товаров со скидкой") AS "Доля товаров со скидкой, %",
    toFloat64("Среднее количество товаров в чеке") AS "Среднее количество товаров в чеке",
    toFloat64("Коэффициент конверсии магазина, %") AS "Коэффициент конверсии магазина, %",
    toFloat64("Средний чек, руб ") AS "Средний чек, руб",
    toFloat64("Средняя выручка на одного посетит") AS "Сред. выручка на посетителя, руб"
FROM postgresql(
    '192.168.214.203:5432', -- IP Greenplum
    'adb',                   -- База
    'datamart_20210101_20210228', -- Представление в Greenplum
    'std15_35',               -- Схема
    'XGcNbG3oH5lKZMWh',      -- Пароль
    'std15_35'                -- Пользователь
);

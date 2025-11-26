CREATE DATABASE IF NOT EXISTS std14_135;

-- создание интегр табл на движке постгрес, читающей из Greenplum

CREATE TABLE IF NOT EXISTS ch_plan_fact_ext
(
    region String,
	matdirec Int32,
	distr_chan String,
	plan_qnt Int32,
	fact_qnt Int32,
	percent_complited Int32,
	top_material String
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'plan_fact_202104', 'std14_135', '1pRDl54AqDKpy5', 'std14_135');

-- проверка
SELECT * FROM ch_plan_fact_ext;

-- посмотрим устройство кластеров

SELECT * FROM system.clusters;

SELECT * FROM system.macros;

-- теперь создадим реплицированные таблицы на всех хостах кластера (пришлось сделать v2, т.к. ch_plan_fact)

DROP TABLE IF EXISTS std14_135.ch_plan_fact ON CLUSTER 'default_cluster';

CREATE TABLE IF NOT EXISTS ch_plan_fact ON CLUSTER 'default_cluster'
(
    region String,
	matdirec Int32,
	distr_chan String,
	plan_qnt Int32,
	fact_qnt Int32,
	percent_complited Int32,
	top_material String
)
ENGINE = ReplicatedMergeTree('/click/ch_plan_fact_v2/{shard}', '{replica}')
ORDER BY percent_complited
SETTINGS index_granularity = 8192;

-- проверка реплик

SELECT * FROM system.replicas
WHERE database = 'std14_135' AND table = 'ch_plan_fact';

-- создадим распределенную табл на основе локальной реплики

CREATE TABLE IF NOT EXISTS ch_plan_fact_distr AS ch_plan_fact
ENGINE = Distributed(default_cluster, std14_135, ch_plan_fact, cityHash64(region, matdirec, distr_chan));

INSERT INTO ch_plan_fact_distr SELECT * FROM ch_plan_fact_ext;

-- проверим данные в лок реплиц табл

SELECT * FROM ch_plan_fact;

-- проверим данные в распр табл

SELECT * FROM ch_plan_fact_distr;

-- проверим шардирование - сколько строк распр-ой табл находится на каждом шарде: 229*203 строк (2 шарда)

SELECT 
    shardNum() AS shard,
    count(*) AS rows
FROM std14_135.ch_plan_fact_distr
GROUP BY shard;

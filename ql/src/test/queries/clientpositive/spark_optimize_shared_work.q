set hive.strict.checks.cartesian.product=false;
set hive.mapred.mode=nonstrict;

-- SORT_QUERY_RESULTS
-- disable hive.spark.optimize.shared.work
set hive.spark.optimize.shared.work=false;
explain select * from (select avg(key) from src) keyTmp,(select count(value) from src) valueTmp;
select * from (select avg(key) from src) keyTmp,(select count(value) from src) valueTmp;

-- enable hive.spark.optimize.shared.work
set hive.spark.optimize.shared.work=true;
explain select * from (select avg(key) from src) keyTmp,(select count(value) from src) valueTmp;
select * from (select avg(key) from src) keyTmp,(select count(value) from src) valueTmp;

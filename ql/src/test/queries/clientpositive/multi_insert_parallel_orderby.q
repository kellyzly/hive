set hive.mapred.mode=nonstrict;
set hive.exec.reducers.bytes.per.reducer=256;
set hive.optimize.sampling.orderby=true;

-- SORT_QUERY_RESULTS

create table e1 (key string, value string);
create table e2 (key string);

--test orderby+multi_insert case
explain
FROM (select key,value from src order by key) a
INSERT OVERWRITE TABLE e1
    SELECT key, value
INSERT OVERWRITE TABLE e2
    SELECT key;

FROM (select key, value from src order by key) a
INSERT OVERWRITE TABLE e1
    SELECT key, value
INSERT OVERWRITE TABLE e2
    SELECT key;

select * from e1;
select * from e2;

--test orderby+multi_insert(contain limit) case
explain
FROM (select key,value from src order by key) a
INSERT OVERWRITE TABLE e1
    SELECT key, value
INSERT OVERWRITE TABLE e2
    SELECT key limit 10;

FROM (select key,value from src order by key) a
INSERT OVERWRITE TABLE e1
    SELECT key, value
INSERT OVERWRITE TABLE e2
    SELECT key limit 10;

select * from e1;
--because the output of e2 is different( just randomly returns 10 query from table a), not output the result of e2

--test orderby+multi_insert(both limit in insert sub-query) case
explain
FROM (select key,value from src order by key) a
INSERT OVERWRITE TABLE e1
    SELECT key, value limit 10
INSERT OVERWRITE TABLE e2
    SELECT key limit 10;

FROM (select key,value from src order by key) a
INSERT OVERWRITE TABLE e1
    SELECT key, value limit 10
INSERT OVERWRITE TABLE e2
    SELECT key order by key limit 10;

select * from e1;
select * from e2;


--test orderby+limit+multi_insert case
explain
FROM (select key,value from src order by key limit 10) a
INSERT OVERWRITE TABLE e1
    SELECT key, value
INSERT OVERWRITE TABLE e2
    SELECT key;

FROM (select key,value from src order by key limit 10) a
INSERT OVERWRITE TABLE e1
    SELECT key, value
INSERT OVERWRITE TABLE e2
    SELECT key;

select * from e1;
select * from e2;

drop table e1;
drop table e2;

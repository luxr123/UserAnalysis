add jar ../lib/tracker-hive-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION parseDateTime AS 'com.tracker.hive.udf.search.GenericUDFParseDateTime';
CREATE TEMPORARY FUNCTION parseId AS 'com.tracker.hive.udf.search.ParseId';
CREATE TEMPORARY FUNCTION parseNum AS 'com.tracker.hive.udf.search.ParseNum';
CREATE TEMPORARY FUNCTION filterEmpty AS 'com.tracker.hive.udf.search.GenericUDTFFilterEmpty';
CREATE TEMPORARY FUNCTION myExplode AS 'com.tracker.hive.udf.search.GenericUDTFExplode';
CREATE TEMPORARY FUNCTION searchCount AS 'com.tracker.hive.udf.search.GenericUDAFSearchCount';
CREATE TEMPORARY FUNCTION searchConditionCount AS 'com.tracker.hive.udf.search.GenericUDAFSearchConditionCount';
CREATE TEMPORARY FUNCTION parseDateId AS 'com.tracker.hive.udf.search.ParseDate';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;

set mapreduce.job.reduces=1;

-- tmp_search_result
insert OVERWRITE table tmp_search_result
  select dt.date_id, web_id, url_id, category_id, search_type, show_type, response_time, result_type, result_value, is_callse, year, month, week, day
  from(
  select parseDateTime(server_time) as dt, web_id, parseId(web_id,url) as url_id, parseId(category) as category_id, search_type,
  show_type, response_time, parseNum(1, cur_page_num) as page_num, parseNum(total_count) as return_num , parseNum(3, response_time) as response_times, is_callse, 
  year, month, week, day
  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
  ) t1
  LATERAL VIEW explode(map(1,page_num, 2,return_num, 3,response_times, 4,dt.time)) t1 as result_type, result_value;

-- fact_search_result
insert into table fact_search_result PARTITION (year, month, week, day)
  select server_date_id, web_id, url_id, category_id, search_type, show_type, result_type, result_value, searchCount(is_callse), SUM(response_time), year, month, week, day
  from tmp_search_result
  group by server_date_id, web_id, url_id, category_id, search_type, show_type, result_type, result_value, year, month, week, day;

set MANAGER_SEARCH=1,company_div,2,cur_company,3,niscohis,4,area,5,full_text,6,nisseniordb,7,company_type,8,company_size,9,core_pos,10,degree,
  11,industry,12,pos_level,13,gender,14,work_years,15,company_text,16,pos_text;
--from(
--  select T2.*, parseDateTime(T1.server_time) as dt, T1.web_id, parseId(T1.web_id,T1.url) as url_id, parseId(T1.category) as category_id, T1.search_type, 
--  T1.show_type, T1.is_callse
--  from manager_search_condition T2 JOIN (select server_time,web_id,url,category,search_type,show_type,is_callse,condition_id,day from apache_search_log) T1 
--  ON (T2.condition_id == T1.condition_id AND T2.day == T1.day) where find_in_set(T2.day, ${hiveconf:days}) > 0
--  ) t1
--  -- tmp_search_condition
--  insert OVERWRITE table tmp_search_condition
--  select dt.date_id, web_id, url_id, category_id, search_type, show_type, condition_type, is_callse, year, month, week, day
--  LATERAL VIEW filterEmpty(map(${hiveconf:MANAGER_SEARCH})) t1 as condition_type
--  -- tmp_search_value
--  insert OVERWRITE table tmp_search_value
--  select dt.date_id, web_id, category_id, search_type, show_type, condition_type, condition_value, is_callse, year, month, week, day
--  LATERAL VIEW myExplode(map(${hiveconf:MANAGER_SEARCH})) t1 as condition_type, condition_value;
  -- tmp_search_value
insert OVERWRITE table tmp_search_value
  select dt.date_id, web_id, url_id, category_id, search_type, show_type, condition_type, condition_value, is_callse, year, month, week, day
  from(
  select T2.*, parseDateTime(T1.server_time) as dt, T1.web_id, parseId(T1.web_id,T1.url) as url_id, parseId(T1.category) as category_id, T1.search_type, 
  T1.show_type, T1.is_callse
  from manager_search_condition T2 JOIN (select server_time,web_id,url,category,search_type,show_type,is_callse,condition_id,day from apache_search_log) T1 
  ON (T2.condition_id == T1.condition_id AND T2.day == T1.day) where find_in_set(T2.day, ${hiveconf:days}) > 0
  ) t1
  LATERAL VIEW myExplode(map(${hiveconf:MANAGER_SEARCH})) t1 as condition_type, condition_value;

set CASE_SEARCH=1,alltext,2,area,3,industry,4,pay,5,commission,6,prepaid,7,exclusive,8,ishrCase,9,spykeywords,10,casename;
--from(
--  select T2.*, parseDateTime(T1.server_time) as dt, T1.web_id, parseId(T1.web_id,T1.url) as url_id, parseId(T1.category) as category_id, T1.search_type, 
--  T1.show_type, T1.is_callse
--  from case_search_condition T2 JOIN (select server_time,web_id,url,category,search_type,show_type,is_callse,condition_id,day from apache_search_log) T1 
--  ON (T2.condition_id == T1.condition_id AND T2.day == T1.day) where find_in_set(T2.day, ${hiveconf:days}) > 0
--  ) t1
--  -- tmp_search_condition
--  insert into table tmp_search_condition
--  select dt.date_id, web_id, url_id, category_id, search_type, show_type, condition_type, is_callse, year, month, week, day
--  LATERAL VIEW filterEmpty(map(${hiveconf:CASE_SEARCH})) t1 as condition_type
--  -- tmp_search_value
--  insert into table tmp_search_value
--  select dt.date_id, web_id, category_id, search_type, show_type, condition_type, condition_value, is_callse, year, month, week, day
--  LATERAL VIEW myExplode(map(${hiveconf:CASE_SEARCH})) t1 as condition_type, condition_value;
  -- tmp_search_value
insert into table tmp_search_value
  select dt.date_id, web_id, url_id, category_id, search_type, show_type, condition_type, condition_value, is_callse, year, month, week, day
  from(
  select T2.*, parseDateTime(T1.server_time) as dt, T1.web_id, parseId(T1.web_id,T1.url) as url_id, parseId(T1.category) as category_id, T1.search_type, 
  T1.show_type, T1.is_callse
  from case_search_condition T2 JOIN (select server_time,web_id,url,category,search_type,show_type,is_callse,condition_id,day from apache_search_log) T1 
  ON (T2.condition_id == T1.condition_id AND T2.day == T1.day) where find_in_set(T2.day, ${hiveconf:days}) > 0
  ) t1
  LATERAL VIEW myExplode(map(${hiveconf:CASE_SEARCH})) t1 as condition_type, condition_value;

-- fact_search_condition
insert into table fact_search_condition PARTITION (year, month, week, day)
  select server_date_id, web_id, url_id, category_id, search_type, show_type, condition_type, searchConditionCount(show_type, condition_type, condition_value), 
  year, month, week, day
  from tmp_search_value where is_callse='true'
  group by server_date_id, web_id, url_id, category_id, search_type, show_type, condition_type, year, month, week, day;

-- fact_search_value
insert into table fact_search_value PARTITION (year, month, week, day)
select server_date_id, web_id, category_id, search_type, show_type, condition_type, condition_value, count, year, month, week, day 
  from (
  select server_date_id, web_id, category_id, search_type, show_type, condition_type, condition_value, count, year, month, week, day, 
  row_number() over (
  distribute by server_date_id, web_id, category_id, search_type, show_type, condition_type 
  sort by server_date_id, web_id, category_id, search_type, show_type, condition_type, count desc) row
  from(
  select server_date_id, web_id, category_id, search_type, show_type, condition_type, condition_value, COUNT(1) as count, year, month, week, day
  from tmp_search_value where is_callse='true'
  group by server_date_id, web_id, category_id, search_type, show_type, condition_type, condition_value, year, month, week, day
  )t)t1
  where row <= 1000;


-- fact_top_response_time
insert into table fact_top_response_time PARTITION(year, month, week, day)
select parseDateId(server_time), web_id, server_time, parseId(category), search_type, response_time, total_count, row, 
  search_param, year, month, week, day
from(
select web_id, server_time, category, search_type, show_type, response_time, total_count, search_param, year, month, week, day, 
  row_number() over (distribute by day sort by day, response_time DESC) row
  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
  )t where row <= 500;


-- fact_top_ip
insert into table fact_top_ip PARTITION(year, month, week, day)
select server_date_id, web_id, category_id, search_type, ip, count, row, year, month, week, day
from(
 select distinct server_date_id, web_id, category_id, search_type, ip, count, year, month, week, day, 
 row_number() over (
 distribute by server_date_id, web_id, category_id, search_type
 sort by server_date_id, web_id, category_id, search_type, count desc) row
 from(
  select web_id, parseDateId(server_time) as server_date_id, parseId(category) as category_id, search_type, ip, year, month, week, day,
  searchCount(is_callse) OVER (PARTITION BY web_id, category, ip, day) as count
  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
  ) t
) t1 where row <= 500;


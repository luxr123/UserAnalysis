add jar ../lib/tracker-hive-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION parsePage AS 'com.tracker.hive.udf.website.ParsePage';
CREATE TEMPORARY FUNCTION parseArea AS 'com.tracker.hive.udf.website.ParseArea';
CREATE TEMPORARY FUNCTION getSession AS 'com.tracker.hive.udf.website.GenericUDAFGetSession';
CREATE TEMPORARY FUNCTION parseSession AS 'com.tracker.hive.udf.website.GenericUDTFParseSession';
CREATE TEMPORARY FUNCTION parseEnv AS 'com.tracker.hive.udf.website.GenericUDTFParseEnv';

--////////////////bd_web_access_log////////////////////////
--自动控制上一轮reduce的数量从而适配bucket的个数
set hive.enforce.bucketing = true; 
set hive.enforce.sorting=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;

set mapreduce.job.reduces=1;

--////////////////fact_web_id (按照server_time进行排序)////////////////////////
insert OVERWRITE table tmp_bd_web_id
select web_id, userid, user_type, ip ,parseArea(country, province, city) as area_id, cookieid ,cookie_create_time ,
  parsePage(web_id, cur_url) as page_id, server_time, ref_type ,ref_domain ,ref_subdomain ,ref_keyword ,
  brower ,os, color_depth,cookie_enabled,language,screen,year, month, week, day
  from bd_web_access_log
  where find_in_set(day, ${hiveconf:days}) > 0 distribute by year, month, week, day, cookieid sort by year, month, week, day, cookieid, server_time;

--////////////////bd_other_url 提取未识别的page url////////////////////////
--insert into table bd_other_url
--select web_id, from_unixtime(CAST(server_time/1000 as bigint),'yyyyMMdd'), page_id
--from tmp_bd_web_id where page_id=1;

--////////////////tmp_bd_web_session////////////////////////
set SESSIONARGS=cookie_create_time, page_id, server_time,ref_type, ref_keyword, ref_domain, ref_subdomain, brower, os, color_depth, cookie_enabled, language, screen;
insert OVERWRITE table tmp_bd_web_session
select web_id, ip, area_id, cookieid, server_date_id ,server_time_id ,ref_type, ref_domain, ref_keyword ,session_page_id,
  day_visitor_type ,week_visitor_type ,month_visitor_type ,year_visitor_type,
  session_time ,total_page ,visit_times ,session_jump_count,
  page_id ,next_page_id ,pv ,entry_page_count ,next_page_count ,out_page_count ,jump_count ,stay_time,
  browser ,os ,color_depth ,cookie_enabled ,language ,screen, year, month, week, day
  from (
  select web_id, ip, area_id, cookieid, getSession(${hiveconf:SESSIONARGS}) as tmpsession, 
  year, month, week, day from tmp_bd_web_id group by web_id, ip, area_id, cookieid, year, month, week, day
  ) t2
  LATERAL VIEW parseSession(tmpsession) t2 as server_date_id ,server_time_id ,ref_type ,ref_keyword ,session_page_id,
  day_visitor_type ,week_visitor_type ,month_visitor_type ,year_visitor_type,
  session_time ,total_page ,visit_times ,session_jump_count,
  page_id ,next_page_id ,pv ,entry_page_count ,next_page_count ,out_page_count ,jump_count ,stay_time,
  browser ,os ,color_depth ,cookie_enabled ,language ,screen, ref_domain;
  
  
from tmp_bd_web_session
--////////////////fact_web_session////////////////////////
insert into table fact_web_session PARTITION (year, month, week, day)
select distinct web_id, ip, parseArea(area_id, 0), parseArea(area_id, 1), area_id, cookieid, server_date_id ,server_time_id ,
  ref_type, ref_domain, ref_keyword ,session_page_id ,
  day_visitor_type ,week_visitor_type ,month_visitor_type ,year_visitor_type,
  session_time ,total_page ,visit_times ,session_jump_count, year, month, week, day 
--////////////////fact_web_page////////////////////////
insert into table fact_web_page PARTITION (year, month, week, day)
select web_id, ip, cookieid, server_date_id , day_visitor_type ,week_visitor_type ,month_visitor_type ,
  year_visitor_type, page_id ,next_page_id , sum(pv), sum(entry_page_count) ,sum(next_page_count), 
  sum(out_page_count), sum(jump_count), sum(stay_time), year, month, week, day 
  group by web_id, ip, cookieid, server_date_id, day_visitor_type ,week_visitor_type ,month_visitor_type ,
  year_visitor_type, page_id, next_page_id, year, month, week, day;
  
  
--////////////////fact_web_env////////////////////////
insert into table fact_web_env PARTITION (year, month, week, day)
select web_id, server_date_id, system_type, system_name, ip, cookieid, day_visitor_type ,week_visitor_type, 
  month_visitor_type ,year_visitor_type, pv, visit_times, session_time, session_jump_count, year, month, week, day  
  from (
  select web_id, server_date_id, ip, cookieid, day_visitor_type ,week_visitor_type ,month_visitor_type ,year_visitor_type,
  browser ,os ,color_depth ,cookie_enabled ,language ,screen, sum(pv) as pv, sum(visit_times) as visit_times, 
  sum(session_time) as session_time, sum(session_jump_count) as session_jump_count, year, month, week, day  
  from tmp_bd_web_session
  group by web_id, ip, cookieid, server_date_id, day_visitor_type ,week_visitor_type ,month_visitor_type, year_visitor_type, 
  browser ,os ,color_depth ,cookie_enabled ,language ,screen, year, month, week, day
  ) t2
  LATERAL VIEW parseEnv(browser ,os ,color_depth ,cookie_enabled ,language ,screen) t2 as system_type, system_name;

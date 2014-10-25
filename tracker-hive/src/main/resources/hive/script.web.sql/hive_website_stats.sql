add jar ../lib/tracker-hive-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION websiteStats AS 'com.tracker.hive.udf.website.WebSiteStatsStorage';
CREATE TEMPORARY FUNCTION websiteKeywordStats AS 'com.tracker.hive.udf.website.WebSiteKeywordStatsStorage';
CREATE TEMPORARY FUNCTION webSiteHourStats AS 'com.tracker.hive.udf.website.WebSiteHourStatsStorage';

-- 全部(1)
select websiteStats(0, web_id, 1, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
  SUM(visit_times), SUM(session_time), SUM(jump_count)), map())
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day;
  
select webSiteHourStats(0, web_id, day, server_time_id, null, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
  SUM(visit_times), SUM(session_time), SUM(jump_count)))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day, server_time_id;
-----------------------------------------------------------------------------------------------------------------------------------------------------

-- 访客(2) -> 新/老访客
select websiteStats(0, web_id, 2, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("vistor_type", day_visitor_type))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, day;

select webSiteHourStats(0, web_id, day, server_time_id, day_visitor_type, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
  SUM(visit_times), SUM(session_time), SUM(jump_count)))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, day, server_time_id;
-----------------------------------------------------------------------------------------------------------------------------------------------------
  
-- 地域(3) -> 国家id + 省id
select websiteStats(0, web_id, 3, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("country_id", country_id, "province_id", province_id))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, country_id, province_id, day;
  
-- 地域(3) -> 国家id + 省id + 市id
select websiteStats(0, web_id, 3, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("country_id", country_id, "province_id", province_id, "city_id",city_id))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, country_id, province_id, city_id, day;
 
----------------------------------------------------------------------------------------------------------------------------------------------------- 
  
-- 来源(4) -> 来源类型（直接访客/搜索引擎/外部链接）
select websiteStats(0, web_id, 4, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("ref_type", ref_type))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, ref_type, day;
  
-- 来源(4) -> 来源类型 + 主域名id (搜索引擎,外部链接 取主域名id top100)
select websiteStats(0, web_id, 4, day, array(uv, ip, pv, visit_times, session_time, jump_count),
  map("ref_type", ref_type, "ref_domain", ref_domain))
from(
select *, row_number() over (
 distribute by web_id, ref_type, day sort by web_id, ref_type, day, pv desc) row
from(
select web_id, ref_type, ref_domain, day, COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count 
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, ref_type, ref_domain, day
  )t
) t1 where row <= 100;

-----------------------------------------------------------------------------------------------------------------------------------------------------
  
-- 访客 + 地域(5) -> 新/老访客 + 国家id + 省id
select websiteStats(0, web_id, 5, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("vistor_type", day_visitor_type, "country_id", country_id, 
  "province_id", province_id))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, country_id, province_id, day;
  
-- 访客 + 地域(5) -> 新/老访客 + 国家id + 省id + 市id
select websiteStats(0, web_id, 5, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("vistor_type", day_visitor_type, "country_id", country_id, 
  "province_id", province_id, "city_id",city_id))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, country_id, province_id, city_id, day;

-----------------------------------------------------------------------------------------------------------------------------------------------------
  
-- 访客 + 来源(6) -> 新/老访客 + 来源类型
select websiteStats(0, web_id, 6, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("vistor_type", day_visitor_type, "ref_type", ref_type))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, ref_type, day;
  
-- 访客 + 来源(6) -> 新/老访客 + 来源类型 + 主域名id (搜索引擎,外部链接 取主域名id top100)
select websiteStats(0, web_id, 6, day, array(uv, ip, pv, visit_times, session_time, jump_count),
  map("vistor_type", day_visitor_type, "ref_type", ref_type, "ref_domain", ref_domain))
from(
select *, row_number() over (
  distribute by web_id, day_visitor_type, ref_type, day sort by web_id, day_visitor_type, ref_type, day, pv desc) row
from(
select web_id, day_visitor_type, ref_type, ref_domain, day, COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, ref_type, ref_domain, day
  )t
) t1 where row <= 100;
  
-----------------------------------------------------------------------------------------------------------------------------------------------------
  
---- 来源 + 地域(7) -> 国家id + 来源类型
--select websiteStats(0, web_id, 7, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
--  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("country_id", country_id, "ref_type", ref_type))
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, country_id, ref_type, day;
--  
---- 来源 + 地域(7) -> 国家id + 省id + 来源类型
--select websiteStats(0, web_id, 7, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
--  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("country_id", country_id, "province_id", province_id, "ref_type", ref_type))
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, country_id, province_id, ref_type, day;
--  
---- 来源 + 地域(7) -> 国家id + 省id + 市id + 来源类型
--select websiteStats(0, web_id, 7, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
--  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("country_id", country_id, "province_id", province_id, "city_id",city_id, 
--  "ref_type", ref_type))
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, country_id, province_id, city_id, ref_type, day;
--  
---- 来源 + 地域(7) -> 国家id + 来源类型 + 主域名id (搜索引擎,外部链接 取主域名id top100)
--select websiteStats(0, web_id, 7, day, array(uv, ip, pv, visit_times, session_time, jump_count),
--  map("country_id", country_id, "ref_type", ref_type, "ref_domain", ref_domain))
--from(
--select *, row_number() over (
--  distribute by web_id, country_id, ref_type, day sort by web_id, country_id, ref_type, day, pv desc) row
--from(
--select web_id, country_id, ref_type, ref_domain, day, COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
--  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, country_id, ref_type, ref_domain, day
--  )t
--) t1 where row <= 100;
--
---- 来源 + 地域(7) -> 国家id + 省id + 来源类型 + 主域名id (搜索引擎,外部链接 取主域名id top100)
--select websiteStats(0, web_id, 7, day, array(uv, ip, pv, visit_times, session_time, jump_count),
--  map("country_id", country_id, "province_id", province_id, "ref_type", ref_type, "ref_domain", ref_domain))
--from(
--select *, row_number() over (
--  distribute by web_id, country_id, province_id, ref_type, day sort by web_id, country_id, province_id, ref_type, day, pv desc) row
--from(
--select web_id, country_id, province_id, ref_type, ref_domain, day, COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
--  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, country_id, province_id, ref_type, ref_domain, day
--  )t
--) t1 where row <= 100;
--
--
---- 来源 + 地域(7) -> 国家id + 省id + 市id +  来源类型 + 主域名id (搜索引擎,外部链接 取主域名id top100)
--select websiteStats(0, web_id, 7, day, array(uv, ip, pv, visit_times, session_time, jump_count),
--  map("country_id", country_id, "province_id", province_id, "city_id",city_id, "ref_type", ref_type, "ref_domain", ref_domain))
--from(
--select *, row_number() over (
--  distribute by web_id, country_id, province_id, city_id, ref_type, day sort by web_id, country_id, province_id, city_id, ref_type, day, pv desc) row
--from(
--select web_id, country_id, province_id, city_id, ref_type, ref_domain, day, COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
--  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, country_id, province_id, city_id, ref_type, ref_domain, day
--  )t
--) t1 where row <= 100;
--  
-------------------------------------------------------------------------------------------------------------------------------------------------------
--
---- 访客 + 地域 + 来源(8) -> 新/老访客 + 国家id + 来源类型
--select websiteStats(0, web_id, 8, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
--  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("vistor_type", day_visitor_type, "country_id", country_id, 
--  "ref_type", ref_type))
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, country_id, ref_type, day;
--  
---- 访客 + 地域 + 来源(8) -> 新/老访客 + 国家id + 省id + 来源类型
--select websiteStats(0, web_id, 8, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
--  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("vistor_type", day_visitor_type, "country_id", country_id, 
--  "province_id", province_id, "ref_type", ref_type))
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, country_id, province_id, ref_type, day;
--  
---- 访客 + 地域 + 来源(8) -> 国家id + 省id + 市id + 来源类型
--select websiteStats(0, web_id, 8, day, array(COUNT(distinct cookieid), COUNT(distinct ip), SUM(total_page),
--  SUM(visit_times), SUM(session_time), SUM(jump_count)), map("vistor_type", day_visitor_type, "country_id", country_id, 
--  "province_id", province_id, "city_id",city_id, "ref_type", ref_type))
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, country_id, province_id, city_id, ref_type, day;
--  
---- 访客 + 地域 + 来源(8) -> 国家id + 来源类型 + 主域名id (搜索引擎,外部链接 取主域名id top100)
--select websiteStats(0, web_id, 8, day, array(uv, ip, pv, visit_times, session_time, jump_count),
--  map("vistor_type", day_visitor_type, "country_id", country_id, "ref_type", ref_type, "ref_domain", ref_domain))
--from(
--select *, row_number() over (
--  distribute by web_id, day_visitor_type, country_id, ref_type, day sort by web_id, day_visitor_type, country_id, ref_type, day, pv desc) row
--from(
--select web_id, day_visitor_type, country_id, ref_type, ref_domain, day, COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
--  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, country_id, ref_type, ref_domain, day
--  )t
--) t1 where row <= 100;
--
---- 访客 + 地域 + 来源(8) -> 国家id + 省id + 来源类型 + 主域名id (搜索引擎,外部链接 取主域名id top100)
--select websiteStats(0, web_id, 8, day, array(uv, ip, pv, visit_times, session_time, jump_count),
--  map("vistor_type", day_visitor_type, "country_id", country_id, "province_id", province_id, "ref_type", ref_type, "ref_domain", ref_domain))
--from(
--select *, row_number() over (
--  distribute by web_id, day_visitor_type, country_id, province_id, ref_type, day 
--  sort by web_id, day_visitor_type, country_id, province_id, ref_type, day, pv desc) row
--from(
--select web_id, day_visitor_type, country_id, province_id, ref_type, ref_domain, day, 
--  COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
--  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, country_id, province_id, ref_type, ref_domain, day
--  )t
--) t1 where row <= 100;
--  
---- 访客 + 地域 + 来源(8) -> 国家id + 省id + 市id +  来源类型 + 主域名id (搜索引擎,外部链接 取主域名id top100)
--select websiteStats(0, web_id, 8, day, array(uv, ip, pv, visit_times, session_time, jump_count),
--  map("vistor_type", day_visitor_type, "country_id", country_id, "province_id", province_id, "city_id",city_id, "ref_type", ref_type, "ref_domain", ref_domain))
--from(
--select *, row_number() over (
--  distribute by web_id, day_visitor_type, country_id, province_id, city_id, ref_type, day 
--  sort by web_id, day_visitor_type, country_id, province_id, city_id, ref_type, day, pv desc) row
--from(
--select web_id, day_visitor_type, country_id, province_id, city_id, ref_type, ref_domain, day, 
--  COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
--  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
--  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, day_visitor_type, country_id, province_id, city_id, ref_type, ref_domain, day
--  )t
--) t1 where row <= 100;

---------------------------------------------------------------------------------------------------------------------------------------------------------------
---- website_keyword_stats
-- 来源 + keyword -> 搜索引擎类型 + 搜索关键词
select websiteKeywordStats(0, web_id, day, array(uv, ip, pv, visit_times, session_time, jump_count), map("ref_keyword", ref_keyword))
from(
select *, row_number() over (
 distribute by web_id, day sort by web_id, day, pv desc) row
from(
select web_id, ref_keyword, day, 
  COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 and ref_type=2 group by web_id, ref_keyword, day
  )t
)t1 where row <= 500;


-- 来源 + keyword -> 搜索引擎类型 + 主域名id + 搜索关键词
select websiteKeywordStats(0, web_id, day, array(uv, ip, pv, visit_times, session_time, jump_count), 
  map("ref_keyword", ref_keyword, "ref_domain", ref_domain))
from(
select *, row_number() over (
 distribute by web_id, day sort by web_id, day, pv desc) row
from(
select web_id, ref_keyword, ref_domain, day,
  COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 and ref_type=2 group by web_id, ref_keyword, ref_domain, day
  )t
)t1 where row <= 500;


-- 访客 + 来源 + keyword -> 新/老访客 +搜索引擎类型 + 搜索关键词
select websiteKeywordStats(0, web_id, day, array(uv, ip, pv, visit_times, session_time, jump_count), 
  map("vistor_type", day_visitor_type, "ref_keyword", ref_keyword))
from(
select *, row_number() over (
 distribute by web_id, day_visitor_type, day sort by web_id, day_visitor_type, day, pv desc) row
from(
select web_id, day_visitor_type, ref_keyword, ref_domain, day, 
  COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 and ref_type=2 group by web_id, day_visitor_type, ref_keyword, ref_domain, day
  )t
)t1 where row <= 500;


-- 访客 + 来源 + keyword -> 新/老访客 +搜索引擎类型 +主域名id + 搜索关键词
select websiteKeywordStats(0, web_id, day, array(uv, ip, pv, visit_times, session_time, jump_count), 
  map("vistor_type", day_visitor_type, "ref_keyword", ref_keyword, "ref_domain", ref_domain))
from(
select *, row_number() over (
 distribute by web_id, day_visitor_type, day sort by web_id, day_visitor_type, day, pv desc) row
from(
select web_id, day_visitor_type, ref_keyword, ref_domain, day, 
  COUNT(distinct cookieid) as uv, COUNT(distinct ip) as ip, SUM(total_page) as pv,
  SUM(visit_times) as visit_times, SUM(session_time) as session_time, SUM(jump_count) as jump_count
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 and ref_type=2 group by web_id, day_visitor_type, ref_keyword, ref_domain, day
  )t
)t1 where row <= 500;


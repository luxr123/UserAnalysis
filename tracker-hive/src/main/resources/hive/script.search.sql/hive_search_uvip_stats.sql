add jar ../lib/tracker-hive-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION parseId AS 'com.tracker.hive.udf.search.ParseId';
CREATE TEMPORARY FUNCTION searchDateStats AS 'com.tracker.hive.udf.search.SearchDateStatsStorage';

set mapreduce.job.reduces=1;

-- 000 search_type(全部)\url_id(全部)\show_type(全部)\category_id
select searchDateStats(0, web_id, day, category_id, null, null, null, COUNT(distinct cookie_id), COUNT(distinct ip), COUNT(1), SUM(response_time), max(response_time))
 from(
  select web_id, parseId(category) as category_id, cookie_id, ip, response_time, day
  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
  ) t
  group by web_id, category_id, day;

-- 100 search_type\url_id(全部)\show_type(全部)\category_id
select searchDateStats(0, web_id, day, category_id, search_type, null, null, COUNT(distinct cookie_id), COUNT(distinct ip), COUNT(1), SUM(response_time), max(response_time))
 from(
  select web_id, parseId(category) as category_id, search_type, cookie_id, ip, response_time,day
  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
  ) t
  group by web_id, category_id, search_type, day;

---- 001 search_type(全部)\url_id(全部)\show_type\category_id
--select searchDateStats(0, web_id, day, category_id,null, null,  show_type, COUNT(distinct cookie_id), COUNT(distinct ip), COUNT(1), SUM(response_time))
-- from(
--  select web_id, parseId(category) as category_id, show_type, cookie_id, ip, response_time, day
--  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
--  ) t
--  group by web_id, category_id, show_type, day;
--
---- 010 search_type(全部)\url_id\show_type(全部)\category_id
--select searchDateStats(0, web_id, day, category_id, null, url_id, null, COUNT(distinct cookie_id), COUNT(distinct ip), COUNT(1), SUM(response_time))
-- from(
--  select web_id, parseId(web_id,url) as url_id, parseId(category) as category_id, cookie_id, ip, response_time,day
--  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
--  ) t
--  group by web_id, url_id, category_id, day;
--
---- 011 search_type(全部)\url_id\show_type\category_id
--select searchDateStats(0, web_id, day, category_id, null, url_id, show_type, COUNT(distinct cookie_id), COUNT(distinct ip), COUNT(1), SUM(response_time))
-- from(
--  select web_id, parseId(web_id,url) as url_id, parseId(category) as category_id, show_type, cookie_id, ip, response_time,day
--  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
--  ) t
--  group by web_id, url_id, category_id, show_type, day;
---- 101 search_type\url_id(全部)\show_type\category_id
--select searchDateStats(0, web_id, day, category_id, search_type, null, show_type, COUNT(distinct cookie_id), COUNT(distinct ip), COUNT(1), SUM(response_time))
-- from(
--  select web_id,  parseId(category) as category_id, search_type, show_type, cookie_id, ip, response_time,day
--  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
--  ) t
--  group by web_id, category_id, search_type, show_type, day;
--
---- 110 search_type\url_id\show_type(全部)\category_id
--select searchDateStats(0, web_id, day, category_id, search_type, url_id, null, COUNT(distinct cookie_id), COUNT(distinct ip), COUNT(1), SUM(response_time))
-- from(
--  select web_id, parseId(web_id,url) as url_id, parseId(category) as category_id, search_type, cookie_id, ip, response_time,day
--  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
--  ) t
--  group by web_id, url_id, category_id, search_type, day;
--
---- 111 search_type\url_id\show_type\category_id
--select searchDateStats(0, web_id, day, category_id, search_type, url_id, show_type, COUNT(distinct cookie_id), COUNT(distinct ip), COUNT(1), SUM(response_time))
-- from(
--  select web_id, parseId(web_id,url) as url_id, parseId(category) as category_id, search_type, show_type, cookie_id, ip, response_time,day
--  from apache_search_log where find_in_set(day, ${hiveconf:days}) > 0
--  ) t
--  group by web_id, url_id, category_id, search_type, show_type, day;

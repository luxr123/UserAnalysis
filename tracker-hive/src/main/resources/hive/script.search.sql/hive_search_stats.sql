add jar ../lib/tracker-hive-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION searchResultStats AS 'com.tracker.hive.udf.search.SearchResultStatsStorage';
CREATE TEMPORARY FUNCTION searchConditionStats AS 'com.tracker.hive.udf.search.SearchConditionStatsStorage';
CREATE TEMPORARY FUNCTION searchValueStats AS 'com.tracker.hive.udf.search.SearchValueStatsStorage';
CREATE TEMPORARY FUNCTION topRtStats AS 'com.tracker.hive.udf.search.TopRtStatsStorage';
CREATE TEMPORARY FUNCTION topIpStats AS 'com.tracker.hive.udf.search.TopIpStatsStorage';

-- SearchResultStatsStorage
select searchResultStats(0, web_id, day, category_id, search_type, url_id, show_type, result_type, result_value, sum(search_count), sum(total_response_time))
  from fact_search_result where find_in_set(day, ${hiveconf:days}) > 0
  group by web_id, day, category_id, search_type, url_id, show_type, result_type, result_value;

-- SearchConditionStatsStorage
select searchConditionStats(0, web_id, day, url_id, category_id, search_type, show_type, condition_type, sum(search_count))
  from fact_search_condition where find_in_set(day, ${hiveconf:days}) > 0
  group by web_id, day, url_id, category_id, search_type, show_type, condition_type;

-- SearchValueStatsStorage (ignore search_type)
select searchValueStats(0, web_id, day, category_id, null, condition_type, condition_value, total_count)
 from (
 select web_id, day, category_id, condition_type, condition_value, total_count, row_number() over (
  distribute by web_id, day, category_id, condition_type
  sort by web_id, day, category_id, condition_type, total_count desc) row
 from(
 select web_id, category_id, condition_type, condition_value, sum(search_count) as total_count, day
  from fact_search_value where find_in_set(day, ${hiveconf:days}) > 0 
  group by web_id, category_id, condition_type, condition_value, day
  )t)t1
  where row <= 1000;

-- SearchValueStatsStorage
select searchValueStats(0, web_id, day, category_id, search_type, condition_type, condition_value, total_count)
 from (
 select web_id, day, category_id, search_type, condition_type, condition_value, total_count, row_number() over (
  distribute by web_id, day, search_type, category_id, condition_type
  sort by web_id, day, category_id, search_type, condition_type, total_count desc) row
 from(
 select web_id, category_id, search_type, condition_type, condition_value, sum(search_count) as total_count, day
  from fact_search_value where find_in_set(day, ${hiveconf:days}) > 0 
  group by web_id, category_id, search_type, condition_type, condition_value, day
  )t)t1
  where row <= 1000;

-- TopRtStats
select topRtStats(0, web_id, day, server_time, category_id, search_type, response_time, total_count, num, search_param)
from(
  select web_id, day, server_time, category_id, search_type, response_time, total_count, search_param,
  row_number() over (distribute by day sort by day, response_time DESC) num
  from fact_top_response_time where find_in_set(day, ${hiveconf:days}) > 0
  )t where num <= 500;

-- TopIpStats
select topIpStats(0, web_id, day, category_id, search_type, ip, total_count, num)
from(
 select web_id, day, category_id, search_type, ip, total_count, row_number() over (
 distribute by web_id, day, category_id, search_type
 sort by web_id, day, category_id, search_type, total_count desc) num
 from(
 select web_id, day, category_id, search_type, ip, sum(search_count) as total_count
  from fact_top_ip where find_in_set(day, ${hiveconf:days}) > 0
  group by web_id, day, category_id, search_type, ip
 )t)t1
 where num <= 500;

add jar ../lib/tracker-hive-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION websitePageStats AS 'com.tracker.hive.udf.website.WebSitePageStatsStorage';
CREATE TEMPORARY FUNCTION websiteEntryPageStats AS 'com.tracker.hive.udf.website.WebSiteEntryPageStatsStorage';
CREATE TEMPORARY FUNCTION websiteSysEnvStats AS 'com.tracker.hive.udf.website.WebSiteSysEnvStatsStorage';
CREATE TEMPORARY FUNCTION webSiteUserStats AS 'com.tracker.hive.udf.website.WebSiteUserStatsStorage';

-- 全部
select websitePageStats(0, web_id, day, null, page_id, array(SUM(pv), COUNT(distinct cookieid), 
  COUNT(distinct ip), SUM(entry_page_count), SUM(next_page_count), SUM(stay_time), SUM(out_page_count)))
  from fact_web_page where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, page_id, day;
  
-- 新老访客
select websitePageStats(0, web_id, day, day_visitor_type, page_id, array(SUM(pv), COUNT(distinct cookieid), 
  COUNT(distinct ip), SUM(entry_page_count), SUM(next_page_count), SUM(stay_time), SUM(out_page_count)))
  from fact_web_page where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, page_id, day_visitor_type, day;

-- 全部  
select websiteEntryPageStats(0, web_id, day, null, page_id, array(SUM(visit_times), COUNT(distinct cookieid), 
  COUNT(distinct ip), SUM(total_page), SUM(jump_count), SUM(session_time)))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, page_id, day;

-- 新老访客
select websiteEntryPageStats(0, web_id, day, day_visitor_type, page_id, array(SUM(visit_times), COUNT(distinct cookieid), 
  COUNT(distinct ip), SUM(total_page), SUM(jump_count), SUM(session_time)))
  from fact_web_session where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, page_id, day_visitor_type, day;

-- 全部
select websiteSysEnvStats(0, web_id, day, null, system_type, system_name, array(SUM(pv), COUNT(distinct cookieid), 
  COUNT(distinct ip), SUM(visit_times), SUM(total_visit_time), SUM(jump_count)))
  from fact_web_env where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, system_type, system_name, day;

-- 新老访客
select websiteSysEnvStats(0, web_id, day, day_visitor_type, system_type, system_name, array(SUM(pv), COUNT(distinct cookieid), 
  COUNT(distinct ip), SUM(visit_times), SUM(total_visit_time), SUM(jump_count)))
  from fact_web_env where find_in_set(day, ${hiveconf:days}) > 0 group by web_id, system_type, system_name, day_visitor_type, day;

-- WebSiteUserStats
select webSiteUserStats(0, web_id, day, user_type, array(COUNT(1), if(user_type==3,COUNT(distinct cookieid),COUNT(distinct userid)), COUNT(distinct ip))) 
  from tmp_bd_web_id group by web_id, user_type, day;

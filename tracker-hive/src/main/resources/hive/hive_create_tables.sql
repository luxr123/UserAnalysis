--///////////////bd_web_access_log///////////////////////
create table bd_web_access_log (
web_id int,
server_time bigint,
client_time bigint,
userid bigint,
user_type int,
cookieid string,
cookie_create_time bigint,
ip string,
country string,
province string,
city string,
user_agent string,
brower string,
os string,
color_depth string,
cookie_enabled string,
language string,
screen string,
referrer string,
cur_url string,
title string,
ref_domain string,
ref_subdomain string,
ref_keyword string,
ref_type int
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

--//////////////tmp_bd_web_id/////////////////////////////
create table tmp_bd_web_id(
web_id int,
userid bigint,
user_type int,
ip string,
area_id int,
cookieid string,
cookie_create_time bigint,
page_id string,
server_time bigint,
ref_type int,
ref_domain string,
ref_subdomain string,
ref_keyword string,
brower string,
os string,
color_depth string,
cookie_enabled string,
language string,
screen string,
year STRING,
month STRING,
week STRING,
day STRING
);

--//////////////bd_other_url/////////////////////////////
create table bd_other_url(
web_id int,
server_time string,
cur_url string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';



--//////////////tmp_web_session/////////////////////////////
create table tmp_bd_web_session (
web_id int,
ip string,
area_id int,
cookieid string,
server_date_id int,
server_time_id int,
ref_type int,
ref_domain string,
ref_keyword string,
session_page_id string,
day_visitor_type int,
week_visitor_type int,
month_visitor_type int,
year_visitor_type int,
session_time bigint,
total_page bigint,
visit_times bigint,
session_jump_count bigint,
page_id string,
next_page_id string,
pv int,
entry_page_count int,
next_page_count int,
out_page_count int,
jump_count int,
stay_time int,
browser string,
os string,
color_depth string,
cookie_enabled string,
language string,
screen string,
year STRING,
month STRING,
week STRING,
day STRING
);


--//////////////fact_web_session/////////////////////////////
create table fact_web_session (
web_id int,
ip string,
country_id int,
province_id int,
city_id int,
cookieid string,
server_date_id int,
server_time_id int,
ref_type int,
ref_domain string,
ref_keyword string,
page_id string,
day_visitor_type int,
week_visitor_type int,
month_visitor_type int,
year_visitor_type int,
session_time bigint,
total_page bigint,
visit_times bigint,
jump_count bigint
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';

--//////////////fact_web_page/////////////////////////////
create table fact_web_page(
web_id int,
ip string,
cookieid string,
server_date_id int,
day_visitor_type int,
week_visitor_type int,
month_visitor_type int,
year_visitor_type int,
page_id string,
next_page_id string,
pv int,
entry_page_count int,
next_page_count int,
out_page_count int,
jump_count int,
stay_time int
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


--//////////////fact_web_env/////////////////////////////
create table fact_web_env(
web_id int,
server_date_id int,
system_type int,
system_name string,
ip string,
cookieid string,
day_visitor_type int,
week_visitor_type int,
month_visitor_type int,
year_visitor_type int,
pv bigint,
visit_times int,
total_visit_time int,
jump_count int
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


--//////////////apache_search_log/////////////////////////////
create table apache_search_log(
server_ip string,
server_time bigint,
status string,
user_agent string,
web_id int,
url string,
category string,
visit_time bigint,
user_id string,
user_type int,
cookie_id string,
cookie_create_time bigint,
ip string,
response_time int,
total_count bigint,
result_count int,
cur_page_num int,
search_type int,
show_type int,
search_param string,
is_callse string,
condition_id string
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`'
NULL DEFINED AS '';

--//////////////manager_search_condition/////////////////////////////
create table manager_search_condition(
condition_id string,
company_div string,
cur_company string,
niscohis int,
area string,
full_text string,
nisseniordb string,
company_type string,
company_size string,
core_pos string,
degree string,
industry string,
pos_level string,
gender string,
work_years string,
company_text string,
pos_text string
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`'
NULL DEFINED AS '';

--//////////////case_search_condition/////////////////////////////
create table case_search_condition(
condition_id string,
alltext string,
area string,
industry string,
pay string,
commission string,
prepaid string,
exclusive string,
ishrCase string,
spykeywords string,
casename string
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`'
NULL DEFINED AS '';

--//////////////tmp_search_result/////////////////////////////
create table tmp_search_result(
server_date_id int,
web_id int,
url_id int,
category_id int,
search_type int,
show_type int,
response_time int,
result_type int,
result_value string,
is_callse string,
year STRING, 
month STRING, 
week STRING, 
day STRING
);


--//////////////fact_search_result/////////////////////////////
create table fact_search_result(
server_date_id int,
web_id int,
url_id int,
category_id int,
search_type int,
show_type int,
result_type int,
result_value string,
search_count bigint,
total_response_time bigint
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


--//////////////tmp_search_condition/////////////////////////////
create table tmp_search_condition(
server_date_id int,
web_id int,
url_id int,
category_id int,
search_type int,
show_type int,
condition_type int,
is_callse string,
year STRING,
month STRING,
week STRING,
day STRING
);

--//////////////fact_search_condition/////////////////////////////
create table fact_search_condition(
server_date_id int,
web_id int,
url_id int,
category_id int,
search_type int,
show_type int,
condition_type int,
search_count bigint
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


--//////////////tmp_search_value/////////////////////////////
create table tmp_search_value(
server_date_id int,
web_id int,
url_id int,
category_id int,
search_type int,
show_type int,
condition_type int,
condition_value string,
is_callse string,
year STRING,
month STRING,
week STRING,
day STRING
);

--//////////////fact_search_value/////////////////////////////
create table fact_search_value(
server_date_id int,
web_id int,
category_id int,
search_type int,
show_type int,
condition_type int,
condition_value string,
search_count bigint
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


--//////////////fact_top_response_time/////////////////////////////
create table fact_top_response_time(
server_date_id int,
web_id int,
server_time bigint,
category_id int,
search_type int,
response_time int,
total_count bigint,
num int,
search_param string
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


--//////////////fact_top_ip/////////////////////////////
create table fact_top_ip(
server_date_id int,
web_id int,
category_id int,
search_type int,
ip string,
search_count bigint,
num int
)
PARTITIONED BY(year STRING, month STRING, week STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '`';


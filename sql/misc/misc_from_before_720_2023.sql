-- MISC QUERIES BEFORE SHUTDOWN 7/20/2023

--
-- attempting to  diagnose the submission deleted/removed and selftext overlap
--
select id, data->>'is_self', 
	data->>'selftext' AS selftext,
	data->>'selftext' is not null AS STNNULL,
	 data->>'selftext' != '[deleted]' AS STDELETED,
	 data->>'selftext' != '[removed]' AS STREMOVED,
	length(data->>'selftext') AS STLEN, 
	(data->>'is_self')::bool as is_text_post,
	(length(data->>'selftext') > 0)::bool has_body_text,
	(length(coalesce(data->>'selftext', '')) > 0)::bool has_body_text2,
	(data->>'selftext' is not null AND 
	 data->>'selftext' != '[deleted]' AND
	 data->>'selftext' != '[removed]' AND
	 length(data->>'selftext') > 0
	),
* 
from submissions 
where id in (127622313, 150917842, 183435983, 139995682, 139965349, 212262945, 212300463);






--
-- more attempts to try and diagnose the removed/deleted/self text
--

with sub_creation as (
	select 
		display_name as subreddit, 
		created_utc,
		date(created_utc) as creation_day
	from subreddits
	where 
	display_name in ('AnimalsFailing', 'MonkeyIsland', 'EarthScience', 'Eskrima', 'LinusTechTips', 'WomensSoccer', 'AskWomenOver30', 'UpliftingNews')
),
sub_activity as (
	select 
		sc.subreddit,
		((ssd.created_utc::date)-creation_day) as age_in_days,
		count(*) as total_submissions,
		count(*) filter (where is_text_post is TRUE  and has_removed_text is FALSE) as count_text_posts,
		count(*) filter (where has_body_text is TRUE and has_removed_text is FALSE) as count_has_body_text,
		count(*) filter (where is_text_post is TRUE and has_deleted_text is TRUE) as text_post_deleted,
		count(*) filter (where is_text_post is TRUE and has_removed_text is TRUE) as text_post_removed,
		count(*) filter (where has_body_text is TRUE and has_deleted_text is TRUE) as body_text_and_deleted,
		count(*) filter (where has_body_text is TRUE and has_removed_text is TRUE) as body_text_and_removed,
		((count(*) filter (where is_text_post is TRUE and has_removed_text is FALSE) )* 100.0)/count(*) as pct_text_posts,
		((count(*) filter (where has_body_text is TRUE and has_removed_text is FALSE))* 100.0) /count(*) as pct_has_body_text,
		((count(*) filter (where has_removed_text is TRUE))* 100.0) /count(*) as pct_removed,
		avg(score) as avg_score,
		avg(num_comments_from_data) as num_comments

	from sub_creation sc
	left join coded_sub_submissions_details ssd on ssd.subreddit = sc.subreddit
	group by sc.subreddit, age_in_days
	order by sc.subreddit, age_in_days
	
)
select * from sub_activity








---
--- continued attempts
---


select id, data->>'is_self' as is_selftext, 
	left(data->>'selftext', 20) AS selftext,
	left(data->>'domain', 40) as url,
	data->>'selftext' is not null AS STNNULL,
	 data->>'selftext' = '[deleted]' AS STDELETED,
	 data->>'selftext' = '[removed]' AS STREMOVED,
	length(data->>'selftext') AS STLEN, 
	(data->>'is_self')::bool as is_text_post,
	(length(data->>'selftext') > 0)::bool has_body_text,
	(length(coalesce(data->>'selftext', '')) > 0)::bool has_body_text2,
	(data->>'selftext' is not null AND 
	 data->>'selftext' != '[deleted]' AND
	 data->>'selftext' != '[removed]' AND
	 length(data->>'selftext') > 0
	),
* 
from submissions
where 
--subreddit in ('LinusTechTips', 'Eskrima', 'MonkeyIsland', 'AskWomenOver30', 'UpliftingNews', 'AnimalsFailing')
--and 
created_utc >= '2015-09-01 00:00:00' and created_utc < '2015-10-01 00:00:00'
and (data->>'selftext' = '[removed]' or data->>'selftext' = '')

--(
--	data->>'is_self' = 'true'
--	OR (data->>'selftext' is not null and (data->>'selftext' ~* '\[(removed|deleted)\]')) 
--	OR data->>'domain' ~* 'self\..*')



select * from submissions where data->>'selftext' is null and created_utc >= '2012-01-01 00:00' and created_utc < '2012-02-01 00:00:00'






---
-- count active users over 30 day windows
--

with sub_creation as (
	select 
		display_name as subreddit, 
		created_utc,
		date(created_utc) as creation_day
	from subreddits
	where 
	display_name in ('AnimalsFailing', 'MonkeyIsland', 'EarthScience', 'Eskrima', 'LinusTechTips', 'WomensSoccer', 'AskWomenOver30', 'UpliftingNews')
),
last_day as (
	select sc.subreddit, 
		max(ssd.date) as last_activity_day
	from sub_creation sc
	left join subreddit_summary_daily ssd on ssd.subreddit = sc.subreddit
	group by sc.subreddit
),
sub_activity as (
	select 
		sc.subreddit,
		sc.created_utc, 
		(last_activity_day-creation_day) as age_in_days,
		(last_activity_time-sc.created_utc) as age,
		ss.unique_authors,
		ss.total_comments,
		ss.total_submissions,
		ss.total_activity,
		ss.total_active_days,
		ss.last_activity_time
	from last_day
	left join sub_creation sc on sc.subreddit = last_day.subreddit
	left join subreddit_summary ss on ss.name = sc.subreddit
),
first_30day_window as (
	select 
		ssd.subreddit,
		sum(total_count) as first30_total_activity,
		sum(comments_total_count) as first30_total_comments,
		sum(submissions_total_count) as first30_total_submissions,
		avg(ssd.unique_authors) as first30_avg_daily_authors,
		count(distinct ssd.date) as distinct_days
	from sub_activity sa
	left join subreddit_summary_daily ssd on ssd.subreddit = sa.subreddit
	where (ssd.date >= created_utc::date and ssd.date < (created_utc::date +30))
	group by ssd.subreddit
),
last_30day_window as (
	select 
		ssd.subreddit,
		sum(total_count) as last30_total_activity,
		sum(comments_total_count) as last30_total_comments,
		sum(submissions_total_count) as last30_total_submissions,
		avg(ssd.unique_authors) as last30_avg_daily_authors,
		count(distinct ssd.date) as distinct_days
	from sub_activity sa
	left join subreddit_summary_daily ssd on ssd.subreddit = sa.subreddit
	where (ssd.date >= ('2017-01-01'::date-30) and ssd.date < '2017-01-01'::date)
	group by ssd.subreddit
)
select 
	sa.*,
	f30w.*,
	l30w.*
from sub_activity sa
left join first_30day_window f30w on f30w.subreddit = sa.subreddit
left join last_30day_window l30w on l30w.subreddit = sa.subreddit








---
--- 
---



select lower(concat('https://redd.it/', (base36_encode(id)))), * from coded_sub_submissions_details
where  has_removed_text is true;








---
---
---


select created_utc, created_utc + make_interval( days => (30*30))  from subreddits where display_name = 'AnimalsFailing'
union all
select created_utc, created_utc + make_interval( days => (30*30))  from subreddits where display_name = 'AskWomenOver30'
union all
select created_utc, created_utc + make_interval( days => (66*30))  from subreddits where display_name = 'Eskrima'
union all
select created_utc, created_utc + make_interval( days => (46*30))  from subreddits where display_name = 'WomensSoccer'
union all
select created_utc, created_utc + make_interval( days => (60*30))  from subreddits where display_name = 'EarthScience'

-- !!!!!!
-- WARNING THIS CALCULATION MAY BE WRONG. LOOK TOWards the following file
-- This data is available in a table across all subreddits in subreddit_30day_rention
-- !!!!!!
--
-- This query as-is does consider missed months into it's calculation. It does this by simply using lag()
-- and the month number to compare the current month to prev month # , to see if it's -1 or not
--
--


with sub_data as (
	select display_name, created_utc
	from subreddits
	where display_name in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'MonkeyIsland', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
--	where display_name in ('MonkeyIsland', 'Eskrima')
),
user_day as (
	select 
		ssd.author,
		ssd.subreddit, 
		(ssd.created_utc::date - sd.created_utc::date)/30 as month_since_creation,
--		1::bit(152) << ((ssd.created_utc::date - sd.created_utc::date)/30) as month_bit,
		1 as submission_flag, 0 as comment_flag
	from coded_sub_submissions_details ssd
	inner join sub_data sd on sd.display_name = ssd.subreddit
	
	union all

	select 
		s1.author,
		s1.subreddit, 
		(s1.created_utc::date - sd.created_utc::date)/30 as month_since_creation,
--		1::bit(152) << ((s1.created_utc::date - sd.created_utc::date)/30) as month_bit,
		0 as submission_flag, 1 as comment_flag
	from s1_coding_comments s1
	inner join sub_data sd on sd.display_name = s1.subreddit
),
total_counts as (
select 
	author, 
	subreddit,
	count(distinct month_since_creation) as total_active_months,
	count(*) as total_activity,
	sum(submission_flag) as total_submissions,
	sum(comment_flag) as total_comments
from user_day
group by author, subreddit
),
distinct_windows as (
	select author, subreddit, month_since_creation, 
		count(*) as total_activity,
		sum(submission_flag) as total_submissions,
		sum(comment_flag) as total_comments
	from user_day
	group by author, subreddit, month_since_creation
	order by author, subreddit, month_since_creation
),
lead_data as (
select
	author,
	subreddit, 
	month_since_creation,
	lag(month_since_creation) over (partition by author, subreddit order by month_since_creation asc) as prev_val,
	total_activity,
	total_submissions,
	total_comments
from distinct_windows
),
totals

select
	subreddit, month_since_creation, count(*) as total_active, 
		sum(case when month_since_creation-1 = prev_val then 1 else 0 end) as active_prev_month,
		NULL as retention_rate,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
		
from lead_data
group by subreddit, month_since_creation
order by subreddit, month_since_creation

-- select 
-- 	author, 
-- 	subreddit,
-- 	array_agg(distinct month_since_creation) as active_months, 
-- 	bit_or(month_bit) active_bits,
-- 	count(distinct month_since_creation) as total_active_months,
-- 	count(*) as total_activity,
-- 	sum(submission_flag) as total_submissions,
-- 	sum(comment_flag) as total_comments
-- from user_day
-- group by author, subreddit
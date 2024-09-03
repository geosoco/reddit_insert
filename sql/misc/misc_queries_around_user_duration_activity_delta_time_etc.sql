--
-- This includes a few queries from a shutdown in dec 23
-- looks at more active users, join dates, etc
-- looks at duration of joins
-- 
-- some use the new activity delta time table for the selected subreddits
--


--
-- Query to check the new submission data numbers against the stuff in the database
--

select  
	cssd.id,
	cssd.created_utc,
	cssd.subreddit,
	cssd.author,
	cssd.score,
	us.score,
	cssd.score - us.score as score_diff,
	us.upvote_ratio,
	cssd.num_comments_from_data,
	us.num_comments,
	comment_count
	
from coded_sub_submissions_details cssd
left join s1_updated_submissions us on us.id = cssd.id
where cssd.score != us.score

order by score_diff asc;




--
-- user joins by (binned) duration on relative days
--

with sub_creation as (
	select 
		display_name as subreddit, 
		created_utc,
		date(created_utc) as creation_day
	from subreddits
	where 
	display_name in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'MonkeyIsland', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
)
select
	sc.subreddit,
	(date-creation_day) as relative_day,
	total_count,
	unique_authors,
	coalesce(count_new_users, 0) as count_new_users,
	coalesce(count_new_users_single_day, 0) as count_new_users_single_day,
	coalesce(count_new_users_7_day, 0) as count_new_users_7_day,
	coalesce(count_new_users_28_day, 0) as count_new_users_28_day,
	coalesce(count_new_users_gte_29_day, 0) as count_new_users_gte_29_day
from sub_creation sc
left join subreddit_new_users_daily snud on snud.subreddit = sc.subreddit
order by subreddit, relative_day asc


--
-- user joins & durations by relative month
--

with sub_creation as (
	select 
		display_name as subreddit, 
		created_utc,
		date(created_utc) as creation_day
	from subreddits
	where 
	display_name in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'MonkeyIsland', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
),
user_enters as (
select
	sc.subreddit,
	extract(epoch from usa.first_activity_time-sc.created_utc)/(24*60*60) as days_since_creation,
	extract(epoch from usa.last_activity_time-usa.first_activity_time)/(24*60*60) as total_duration_days,
	total_activity,
	case when first_activity_time = first_comment_time then 'c' else 's' end as first_activity_type
from sub_creation sc
left join user_subreddit_activity usa on usa.subreddit = sc.subreddit
),
user_enter_data as (
	select 
		ue.*,
		floor(days_since_creation/30) as rel_month,
		case
			when total_duration_days < 1 then '<1'
			when total_duration_days < 31 then '<31'
			when total_duration_days < 181 then '<181'
			when total_duration_days < 366 then '<366'
			else '>365'
		end as duration_bin
	from user_enters ue
)
	select 
		subreddit,
		rel_month,
		duration_bin,
		count(*)
	from user_enter_data
	group by subreddit, rel_month, duration_bin





--
-- average duration of more active users by subreddit
--

select
	subreddit, 
	count(*),
	avg(extract(epoch from last_activity_time-first_activity_time)/(24*60*60)) as average_duration
from user_subreddit_activity
where subreddit in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
and total_activity > 10
group by subreddit;



--
-- # of active users (total_activity > 10) joining on relative days
--

with sub_creation as (
	select 
		display_name as subreddit, 
		created_utc,
		date(created_utc) as creation_day
	from subreddits
	where 
	display_name in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
)
select
	sc.subreddit, 
	floor(extract(epoch from usa.first_activity_time-sc.created_utc)/(24*60*60)) as days_since_creation,
	count(*)
from sub_creation sc
left join user_subreddit_activity usa on usa.subreddit = sc.subreddit
where total_activity > 10
group by sc.subreddit, floor(extract(epoch from usa.first_activity_time-sc.created_utc)/(24*60*60));




--
-- selected subreddit ages
--

select 
	display_name as subreddit, 
	created_utc,
	date(created_utc) as creation_day,
	age('2017-01-01'::timestamp,created_utc)  as age
from subreddits
where 
display_name in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')






--
-- ranked active users in each subreddit
--

with sub_users as (
	select 
		row_number() over (partition by subreddit order by total_activity desc) as rank,
		usa.*
	from user_subreddit_activity usa
	where subreddit in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
	and total_activity > 1
	and author != '[deleted]'
)

select *
from sub_users
where rank <= 25
order by subreddit, rank



--
-- average delta time by subreddit
-- 

with times as (
	select subreddit, author, extract('epoch' from delta_time)/(60*60*24) delta_time
	from s1_user_subreddit_activity_delta_time
	where delta_time is not null
)
select
	subreddit, avg(delta_time) as avg_delta
from times
group by subreddit



--
-- 
--


with times as (
	select subreddit, author, avg(extract('epoch' from delta_time))/(60*60*24) as mean_time, max(extract('epoch' from delta_time))/(60*60*24) as max_delta
	from s1_user_subreddit_activity_delta_time
	where delta_time is not null
	group by subreddit, author
)
select
	avg(mean_time) as avg_delta,
	avg(max_delta) as avg_max
from times
group by subreddit




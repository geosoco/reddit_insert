--
-- submissions_monthly_engagement
--
-- rough engagement estimates of submission data across active subreddits
--


--drop table if exists submissions_monthly_engagement;

with 
--subreddit_list as (
--	select name from subreddit_summary
--	where total_activity >= 1000 and unique_authors >= 10
--),
subreddit_list as (
	select name from subreddit_summary
	where total_activity >= 1000 and unique_authors >= 10
),
raw_submissions as (
select
	sl.name as subreddit,
	id,
	created_utc,
	date_trunc('month', created_utc) as month,
	(data->>'score')::int as score,
	(data->>'num_comments')::int as num_comments
from subreddit_list sl 
left join submissions s on s.subreddit = sl.name
)

select
	subreddit,
	month,
	count(id) filter(where score>1) as gt_1,
	count(id) filter(where score>0) as gt_0,
	count(id) filter(where num_comments>1) as has_comments,
	count(id) filter(where score>1 or num_comments>1) as num_engaged,
	count(id) filter(where score>0 or num_comments>1) as num_engaged_sc_gt0,
	count(*) as total_submissions
into table submissions_monthly_engagement
from raw_submissions
group by subreddit, month



create index on submissions_monthly_engagement(subreddit);
create index on submissions_monthly_engagement(month);
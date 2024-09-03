--
--
-- s3_subreddit_monthly_activity
--
--

drop table if exists s3_subreddit_monthly_activity;

select
	subreddit, 
	month_year,
	count(distinct author) as unique_authors,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
into s3_subreddit_monthly_activity
from s3_user_sub_activity_monthly_activity
group by subreddit, month_year;


grant select on s3_subreddit_monthly_activity to public;
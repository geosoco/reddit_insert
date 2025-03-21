--
--
-- s3_2016_subreddit_activity
--
--
-- combined data for all of 2016 across all subreddits
--



select
	subreddit,
	count(distinct author) as unique_authors,
	sum(total_items) as total_activity,
	sum(num_submissions) as total_submissions,
	sum(num_comments) as total_comments

into s3_2016_subreddit_activity
from user_subreddit_daily_summary
where date >= '2016-01-01'
group by subreddit;



grant select on s3_2016_subreddit_activity to public;

create index on s3_2016_subreddit_activity(unique_authors);
create index on s3_2016_subreddit_activity(subreddit);
create index on s3_2016_subreddit_activity(total_activity);
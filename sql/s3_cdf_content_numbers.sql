-- 
-- 
-- s3_cdf_content_numbers
-- 
-- 

drop table if exists s3_cdf_content_numbers;

with c_data as (
	select
		subreddit,
		'comments' as content_type,
		total_comments as amount, 
		count(*) as count
	from user_subreddit_activity usa
	where subreddit in ('hillaryclinton', 'SandersForPresident', 'The_Donald')
	and author != '[deleted]'
	group by subreddit, total_comments
	order by subreddit, total_comments asc
),
s_data as (
	select
		subreddit,
		'submissions' as content_type,
		total_submissions as amount, 
		count(*) as count
	from user_subreddit_activity usa
	where subreddit in ('hillaryclinton', 'SandersForPresident', 'The_Donald')
	and author != '[deleted]'
	group by subreddit, total_submissions
	order by subreddit, total_submissions asc
),
combined as (
	select * from c_data
	union all 
	select * from s_data
)
select * 
into s3_cdf_content_numbers
from combined;


grant select on s3_cdf_content_numbers to public;
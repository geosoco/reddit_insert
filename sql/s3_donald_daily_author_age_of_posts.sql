--
--
-- s3_donald_daily_author_age_of_posts
--
--
-- Looks at the author age at the time content was created. The results here weren't that interesting
--
--




drop table if exists s3_donald_daily_author_age_of_posts;

with 
daily_joins as (
	select
		subreddit,
		author,
		first_activity_time
	from user_subreddit_activity
	where subreddit in ('The_Donald', 'SandersForPresident', 'hillaryclinton')
),
daily_activity as (
	select
		uca.subreddit,
		uca.author,
		created_utc,
		date_trunc('day', created_utc) as created_date,
		extract(day from (created_utc - dj.first_activity_time)) as author_age
	from user_combined_activity uca
	left join daily_joins dj on dj.author = uca.author and dj.subreddit = uca.subreddit
	where uca.subreddit in ('The_Donald', 'SandersForPresident', 'hillaryclinton') 
	and uca.author not in ('[deleted]', 'AutoModerator')
)
select
	subreddit,
	created_date,
	author_age,
	count(*)
into s3_donald_daily_author_age_of_posts
from daily_activity da
group by subreddit, created_date, author_age
order by subreddit, created_date asc, author_age asc;


grant select on s3_donald_daily_author_age_of_posts to public;

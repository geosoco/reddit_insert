--
-- s2_user_subreddit_activity_30day
--
--
-- aggregate table for user participation in the study 2 subs
--
--


drop table if exists s2_user_subreddit_activity_30day;

with year_subs as (
	select display_name, created_utc
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' 
),
active_users as (
	select subreddit, 
		(extract(epoch from (usds.date - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
		author,
		num_submissions,
		num_comments,
		total_items as total_activity
	from year_subs ys
	left join user_subreddit_daily_summary usds on usds.subreddit = ys.display_name

)
select
 subreddit,
 creation_delta_months,
 author,
 sum(num_submissions) as num_submissions,
 sum(num_comments) as num_comments,
 sum(total_activity) as total_activity
into s2_user_subreddit_activity_30day
from active_users
group by subreddit, creation_delta_months, author;


grant select on s2_user_subreddit_activity_30day to public;

create index on s2_user_subreddit_activity_30day(subreddit);
create index on s2_user_subreddit_activity_30day(author);
create index on s2_user_subreddit_activity_30day(subreddit, author);
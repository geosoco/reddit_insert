--
-- s2_user_subreddit_joins
--
--
--
--
--



drop table if exists s2_user_subreddit_joins;


with year_subs as (
	select display_name as subreddit, created_utc
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' 
),
active_users as (
	select ys.subreddit, 
		usa.author,
		usa.first_activity_time,
		(extract(epoch from (usa.first_activity_time - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
		row_number() over (partition by ys.subreddit order by first_activity_time asc) as account_num
	from year_subs ys
	left join user_subreddit_activity usa on ys.subreddit = usa.subreddit
	where usa.author is not null and usa.author != '[deleted]'
	order by ys.subreddit, account_num asc
)
select
	*
into s2_user_subreddit_joins
from active_users ua
where account_num <= 1000;



grant select on s2_user_subreddit_joins to public;


create index on s2_user_subreddit_joins(subreddit);
create index on s2_user_subreddit_joins(account_num);

	
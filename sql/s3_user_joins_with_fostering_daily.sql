--
--
-- s3_user_joins_with_fostering_daily
--
--
--


drop table if exists s3_user_joins_with_fostering_daily;


with
author_joins as (
	select
		date_trunc('day', first_activity_time) as date,
		case when first_fostering_month is not null then 'foster'
		when num_days >= 1 then 'multiday'
		else 'day' end as duration,
		*
	from s3_user_joins_with_fostering_monthly
)
select
	subreddit,
	date,
	count(*) as total_count,
	count(*) filter (where duration = 'foster') as total_fostering,
	count(*) filter (where duration = 'multiday') as total_multiday,
	count(*) filter (where duration = 'day') as total_day


into s3_user_joins_with_fostering_daily
from author_joins aj
group by subreddit, date
order by subreddit, date asc;


grant select on s3_user_joins_with_fostering_daily to public;


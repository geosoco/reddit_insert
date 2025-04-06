--
--
-- s3_user_joins_with_fostering_hourly
--
-- NOTE: This doesn't seem to be used
--


drop table if exists s3_user_joins_with_fostering_hourly;


with
author_joins as (
	select
		date_trunc('hour', first_activity_time) as day_hour,
		case when first_fostering_month is not null then 'foster'
		when num_days >= 1 then 'multiday'
		else 'day' end as duration,
		*
	from s3_user_joins_with_fostering_monthly
)
select
	subreddit,
	day_hour,
	count(*) as total_count,
	count(*) filter (where duration = 'foster') as total_fostering,
	count(*) filter (where duration = 'multiday') as total_multiday,
	count(*) filter (where duration = 'day') as total_day


into s3_user_joins_with_fostering_hourly
from author_joins aj
group by subreddit, day_hour
order by subreddit, day_hour asc;


grant select on s3_user_joins_with_fostering_hourly to public;


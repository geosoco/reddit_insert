--
--
-- s3_promos_and_joins_summary
--
--
--
-- When looking at 3 days this takes about 40-45 minutes to run
-- For the current 7 day window, it's 2-3ish hours
--



drop table if exists s3_promos_and_joins_summary;

with join_days as (
select
*,
case when first_fostering_month is not null then 3
when num_days >= 1 then 2
else 1 end as duration

from s3_user_joins_with_fostering
),

promos as (
select
	id, 
	mentioned_sub_name,
--	subreddit,
	created_utc,
	case when num_crossposts > 0 then 3
	when num_links > 0 then 2
	when num_mentions > 0 then 1 
	else 0 end as promo_type
	
	
from s3_inbound_adv_content_combined imcf
),
promos_and_joins as (
	select
	 jd.subreddit,
	 p.id, p.created_utc, 
	 jd.duration,
	 cast(extract(epoch from p.created_utc - jd.first_activity_time)/3600 as int) as num_hours
	from promos p, 
	join_days jd
	where p.mentioned_sub_name = jd.subreddit
	and p.created_utc < '2016-09-01' and jd.first_activity_time <= (('2016-09-01')::date + interval '192' hour)
	and jd.first_activity_time >=  (p.created_utc - interval '192' hour)  and  jd.first_activity_time < (p.created_utc + interval '192' hour)
),
summaries as (

	select subreddit, id, num_hours, 
		count(*) as total_promos,
		count(*) filter (where duration = 1) as total_day_joins,
		count(*) filter (where duration = 2) as total_multiday_joins,
		count(*) filter (where duration = 3) as total_foster_joins
	from promos_and_joins
	where created_utc < '2016-09-01'
	group by subreddit, id, num_hours
	order by subreddit, id, num_hours asc
)
select * 
into s3_promos_and_joins_summary
from summaries;




--extract(epoch from p.created_utc - jd.first_activity_time)/3600 >= -72 and extract(epoch from p.created_utc - jd.first_activity_time)/3600 < 72;


grant select on s3_promos_and_joins_summary to public;


create index on s3_promos_and_joins_summary(subreddit);
create index on s3_promos_and_joins_summary(id);
--create index on s3_promos_and_joins_summary(created_utc);
create index on s3_promos_and_joins_summary(num_hours);
--create index on s3_promos_and_joins_summary(duration);

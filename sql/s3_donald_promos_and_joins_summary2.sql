--
--
-- s3_donald_promos_and_joins_summary2
--
--
--
-- When looking at 3 days this takes about 40-45 minutes to run
-- For the current 7 day window, it's 3ish hours
--





drop table if exists s3_donald_promos_and_joins_summary2;

with join_days as (
select
*,
case when first_fostering_month is not null then 3
when num_days >= 1 then 2
else 1 end as duration

from s3_user_joins_with_fostering
where subreddit = 'The_Donald'
),

promos as (
select
	id, 
	subreddit,
	created_utc
	
	
from s3_inbound_adv_content_combined imcf
where mentioned_sub_name = 'The_Donald' 
),
promos_and_joins as (
	select
	 p.*,
	 jd.duration,
	 cast(extract(epoch from p.created_utc - jd.first_activity_time)/3600 as int) as num_hours
	from promos p, 
	join_days jd
	where p.created_utc < '2016-09-01' and jd.first_activity_time <= (('2016-09-01')::date + interval '7' day)
	and jd.first_activity_time >=  (p.created_utc - interval '7' day)  and  jd.first_activity_time < (p.created_utc + interval '7' day)
),
summaries as (

	select id,  num_hours, 
		count(*) as total_promos,
		count(*) filter (where duration = 1) as total_day_joins,
		count(*) filter (where duration = 2) as total_multiday_joins,
		count(*) filter (where duration = 3) as total_foster_joins
	from promos_and_joins
	where created_utc < '2016-09-01'
	group by id, num_hours
	order by id, num_hours asc
)
select * 
into s3_donald_promos_and_joins_summary2
from summaries;




--extract(epoch from p.created_utc - jd.first_activity_time)/3600 >= -72 and extract(epoch from p.created_utc - jd.first_activity_time)/3600 < 72;


grant select on s3_donald_promos_and_joins_summary2 to public;


create index on s3_donald_promos_and_joins_summary2(id);
--create index on s3_donald_promos_and_joins_summary2(created_utc);
create index on s3_donald_promos_and_joins_summary2(num_hours);
--create index on s3_donald_promos_and_joins_summary2(duration);

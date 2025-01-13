--
--
-- s3_donald_promos_and_joins
--
--
-- STOP: THIS IS OLD
-- 
-- Takes 4-6 hours, and is only an intermediate table
-- The s3_donald_promos_and_joins_summary2 does this in like 40 minutes without the intermediate table
-- by reducing the amount of processed data and summarizing immediately
--

drop table if exists s3_donald_promos_and_joins;

with join_days as (
select
*,
case when first_fostering_month is not null then 3
when num_days >= 1 then 2
else 1 end as duration

from s3_donald_user_joins_with_fostering
),

promos as (
select
	id, 
	subreddit,
	created_utc,
	case when num_crossposts > 0 then 3
	when num_links > 0 then 2
	when num_mentions > 0 then 1 
	else 0 end as promo_type
	
	
from s3_inbound_adv_content_combined imcf
where mentioned_sub_name = 'The_Donald' 
)

select
 p.*,
 jd.first_activity_time,
 jd.author as join_author,
 jd.duration,
 cast(extract(epoch from p.created_utc - jd.first_activity_time)/3600 as int) as num_hours
into s3_donald_promos_and_joins
from promos p, 
join_days jd
where p.created_utc + interval '3' day > jd.first_activity_time and p.created_utc - interval '3' day <= jd.first_activity_time;

--extract(epoch from p.created_utc - jd.first_activity_time)/3600 >= -72 and extract(epoch from p.created_utc - jd.first_activity_time)/3600 < 72;


grant select on s3_donald_promos_and_joins to public;


create index on s3_donald_promos_and_joins(created_utc);
create index on s3_donald_promos_and_joins(num_hours);
create index on s3_donald_promos_and_joins(duration);

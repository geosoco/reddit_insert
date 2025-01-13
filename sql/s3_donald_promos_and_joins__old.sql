--
--
--
-- s3_donald_promos_and_joins
--
--
-- STOP - THIS IS VERY OLD AND SHOULDN'T BE USED
--
-- This takes about 6 hours to run, and it seems to ignore the indexes
-- It also produces 2.8 billion rows which takes ~35-40 mins just to "count"
-- 
--




drop table if exists s3_donald_promos_and_joins;

with join_days as (
select
*,
case when first_fostering_month is not null then 'foster'
when num_days >= 1 then 'multiday'
else 'day' end as duration

from s3_donald_user_joins_with_fostering
),

promos as (
select
	comment_id,
	submission_id,
	subreddit,
	created_utc,
	case when comment_id is not null then 'comment' else 'submission' end as promo_source
	
from s3_inbound_adv_content_combined imcf
where mentioned_sub_name = 'The_Donald' 
)

select
 p.*,
 jd.author,
 jd.first_activity_time,
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

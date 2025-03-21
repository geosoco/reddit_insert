--
--
-- s3_donald_promos_and_joins_summary
--
--
-- STOP: THIS IS OLD
-- 
-- Takes around 40 minutes, but the singular table is better to use than this.
-- 
-- 
--

drop table if exists s3_donald_promos_and_joins_summary;


select
promo_type, duration, num_hours, count(*) as num_joins
into s3_donald_promos_and_joins_summary
from s3_donald_promos_and_joins
where created_utc < '2016-09-01'
group by promo_type, duration, num_hours
order by promo_type, duration, num_hours asc;


grant select on s3_donald_promos_and_joins_summary to public;
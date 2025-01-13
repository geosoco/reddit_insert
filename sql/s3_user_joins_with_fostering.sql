--
--
-- s3_user_joins_with_fostering
--
-- NOTE: This leverages the creation-relative sequence table, so fostering is not determined by 
-- calendar months. This means the first_fostering_month is a number instead of a date
--


drop table if exists s3_user_joins_with_fostering;

with
author_joins as (
	select 
		subreddit,
		author,
		first_activity_time,
		last_activity_time,
		total_activity,
		extract(day from last_activity_time - first_activity_time) as num_days
		
from user_subreddit_activity 
where subreddit in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
)
,
fostering_authors as (
-- NOTE: This is pulling from the creation-relative sequence table, 
-- so fostering data can exist through early november in 2016 in some specific cases	
	select 
		subreddit,
		author,
		min(first_delta_month) as first_delta_month,
		sum(total_months) as fostered_months

	from s3_sub_user_sequence_data2 susd
	where total_months >= 3
	group by subreddit, author
)
select 
aj.*,
fa.first_delta_month as first_fostering_month,
fa.fostered_months
into s3_user_joins_with_fostering
from author_joins aj
left join fostering_authors fa on fa.subreddit = aj.subreddit and fa.author = aj.author;


grant select on s3_user_joins_with_fostering to public;


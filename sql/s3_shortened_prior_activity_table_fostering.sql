--
--
-- s3_shortened_prior_activity_table_fostering
--
--
-- Find author history of the most active authors
-- 
--
--


drop table if exists s3_shortened_prior_activity_table_fostering;

with 
fostering_authors as (
	select 
		distinct subreddit,
		author,
		case when first_fostering_month is not null then 'foster'
		when num_days >= 1 then 'multiday'
		else 'day' end as duration			

	from s3_user_joins_with_fostering_monthly uj
),
join_subs as (

	select
		political_sub_first_content, 
		fa.author,
		c_id,
		s_id,
		duration,
		date_trunc('day', sub_first_activity_time) as join_date,
		(extract(epoch from (sub_first_activity_time - created_utc))::bigint) / (3600*24)::int as days_before_join,
		uabpc.subreddit
	from 
	fostering_authors fa
	left join s3_user_activity_before_join_political_community uabpc on fa.author = uabpc.author and fa.subreddit = uabpc.political_sub_first_content
	where content_number > 1 and ( 
	((extract(epoch from (sub_first_activity_time - created_utc))::bigint) / (3600*24)::int) < 30 and content_number <= 1001)
),
unique_subs as (
	select
		political_sub_first_content, author, join_date, duration, subreddit, count(*) as total_content
	from join_subs
	group by political_sub_first_content, author, join_date, duration, subreddit
)
select
	political_sub_first_content, join_date, duration, subreddit, count(distinct author) as unique_authors, sum(total_content) as total_activity
into s3_shortened_prior_activity_table_fostering
from unique_subs us
group by political_sub_first_content, join_date, duration, subreddit
order by political_sub_first_content, join_date, unique_authors desc;


grant select on s3_shortened_prior_activity_table_fostering to public;
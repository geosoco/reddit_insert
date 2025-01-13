--
-- s3_shortened_prior_activity_table
--
--
-- don't think this was used, has been replaced by s3_shortened_prior_activity_table2
--
--


drop table if exists s3_shortened_prior_activity_table;

with join_subs as (

	select
		political_sub_first_content, 
		author,
		c_id,
		s_id,
		date_trunc('day', sub_first_activity_time) as join_date,
		subreddit
	from s3_user_activity_before_join_political_community
	where content_number > 1 and content_number <= 51

),
unique_subs as (
	select
		political_sub_first_content, author, join_date, subreddit, count(*) as total_content
	from join_subs
	group by political_sub_first_content, author, join_date, subreddit
)
select
	political_sub_first_content, join_date, subreddit, count(distinct author) as unique_authors, sum(total_content) as total_content
into s3_shortened_prior_activity_table
from unique_subs us
group by political_sub_first_content, join_date, subreddit
order by political_sub_first_content, join_date, unique_authors desc;


grant select on s3_shortened_prior_activity_table to public;

-- select
-- 	political_sub_first_content,
-- 	join_date,
-- 	count(distinct author)



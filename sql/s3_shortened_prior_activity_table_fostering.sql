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

with eligible_authors as (
	select
		distinct subreddit, author
	from s3_sub_user_monthly_sequence_data
	where total_months >= 3
	and author not in ('[deleted]', 'AutoModerator')

),
join_subs as (

	select
		political_sub_first_content, 
		ea.author,
		c_id,
		s_id,
		date_trunc('day', sub_first_activity_time) as join_date,
		uabpc.subreddit
	from eligible_authors ea
	left join s3_user_activity_before_join_political_community uabpc on uabpc.political_sub_first_content = ea.subreddit and uabpc.author = ea.author
	where content_number > 1 and content_number <= 11

),
unique_subs as (
	select
		political_sub_first_content, author, join_date, subreddit, count(*) as total_content
	from join_subs
	group by political_sub_first_content, author, join_date, subreddit
)
select
	political_sub_first_content, join_date, subreddit, count(distinct author) as unique_authors, sum(total_content) as total_content
into s3_shortened_prior_activity_table_fostering
from unique_subs us
group by political_sub_first_content, join_date, subreddit
order by political_sub_first_content, join_date, unique_authors desc;


grant select on s3_shortened_prior_activity_table_fostering to public;
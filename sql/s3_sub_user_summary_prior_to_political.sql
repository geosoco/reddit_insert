--
-- s3_sub_user_summary_prior_to_political
--
-- This is a summary of user activity of prior activity for user and subs. 
--
-- This was used to calculate how many people were active in various subreddits
--
--
--
--


drop table if exists s3_sub_user_summary_prior_to_political;

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
agg_data as (
select 
 political_sub_first_content,
 author,
 sub_first_activity_time,
 subreddit,
 count(c_id) filter (where c_id is not null) comment_count,
 count(s_id) filter (where s_id is not null) submission_count,
 sum(score) as cumulative_score,
 sum(score) filter (where c_id is not null) as cumulative_comment_score,
 sum(score) filter (where s_id is not null) as cumulative_submission_score,
 avg(score) as avg_score
from s3_user_activity_before_join_political_community
group by  political_sub_first_content, author, sub_first_activity_time, subreddit
)
select
 ag.*,
 fa.duration
into s3_sub_user_summary_prior_to_political 
from agg_data ag
left join fostering_authors fa on fa.subreddit = ag.political_sub_first_content and fa.author = ag.author;


grant select on s3_sub_user_summary_prior_to_political to public;

create index on s3_sub_user_summary_prior_to_political(political_sub_first_content);
create index on s3_sub_user_summary_prior_to_political(subreddit);
create index on s3_sub_user_summary_prior_to_political(political_sub_first_content, subreddit);
create index on s3_sub_user_summary_prior_to_political(author);

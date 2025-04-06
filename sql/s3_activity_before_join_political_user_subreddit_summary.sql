--
--
-- s3_activity_before_join_political_user_subreddit_summary
--
--
--
-- This is a summary table that has the user-subreddit activity counts before joining a political subreddit in the 30-day window prior to joining (up to 1000 pieces)
--
-- It was intended to both generate histograms of the activity as well as the network graphs
--



drop table if exists s3_activity_before_join_political_user_subreddit_summary;

select
	political_sub_first_content, author, subreddit, 
	date_trunc('day', sub_first_activity_time) as join_date,
	count(*) as total_content,
	count(*) filter (where score > 0) as total_positive_content,
	
	count(*) filter (where c_id is not null) as total_comments,
	count(*) filter (where s_id is not null) as total_submissions,
	
	count(*) filter (where c_id is not null and score > 0) as total_positive_comments,
	count(*) filter (where s_id is not null and score > 0) as total_positive_submissions

into s3_activity_before_join_political_user_subreddit_summary
from s3_user_activity_before_join_political_community 
group by political_sub_first_content, author, subreddit, join_date;


grant select on s3_activity_before_join_political_user_subreddit_summary to public;


create index on s3_activity_before_join_political_user_subreddit_summary(political_sub_first_content);
create index on s3_activity_before_join_political_user_subreddit_summary(subreddit);
create index on s3_activity_before_join_political_user_subreddit_summary(author);
create index on s3_activity_before_join_political_user_subreddit_summary(join_date);

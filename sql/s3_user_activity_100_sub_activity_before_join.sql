--
--
-- s3_user_activity_100_sub_activity_before_join
--
-- A more focused look at the active subreddits before a user joins the political subreddit
--
--


select
	political_sub_first_content,
	author,
	subreddit,
	count(*) as total_activity,
	count(*) filter (where c_id is not null) as num_comments,
	count(*) filter (where s_id is not null) as num_submissions,

	min(created_utc) as threshold_content_time,
	sum(score) as total_score,
	count(*) filter (where deleted is true) as num_deleted,
	count(*) filter (where removed is true) as num_removed
	
into s3_user_activity_100_sub_activity_before_join
from s3_user_activity_before_join_political_community
where content_number < 101 and content_number > 1
group by 	political_sub_first_content, author, subreddit;


grant select on s3_user_activity_100_sub_activity_before_join to public;

create index on s3_user_activity_100_sub_activity_before_join(author);
create index on s3_user_activity_100_sub_activity_before_join(political_sub_first_content);
create index on s3_user_activity_100_sub_activity_before_join(subreddit);
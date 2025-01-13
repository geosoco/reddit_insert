--
--
-- s3_user_activity_10_sub_activity_before_join
--
--
--
--
--
--


drop table if exists s3_user_activity_10_sub_activity_before_join;

select
	political_sub_first_content,
	sub_first_activity_time,
	author,
	subreddit,
	count(*) as total_activity,
	count(*) filter (where c_id is not null) as num_comments,
	count(*) filter (where s_id is not null) as num_submissions,

	min(created_utc) as threshold_content_time,
	sum(score) as total_score,
	count(*) filter (where deleted is true) as num_deleted,
	count(*) filter (where removed is true) as num_removed
	
into s3_user_activity_10_sub_activity_before_join
from s3_user_activity_before_join_political_community
where content_number < 12 and content_number > 1
group by 	political_sub_first_content, sub_first_activity_time, author, subreddit;


grant select on s3_user_activity_10_sub_activity_before_join to public;

create index on s3_user_activity_10_sub_activity_before_join(author);
create index on s3_user_activity_10_sub_activity_before_join(political_sub_first_content);
create index on s3_user_activity_10_sub_activity_before_join(subreddit);
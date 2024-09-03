--
-- submissions_monthly_engagement2
--
-- rough engagement estimates of submission data across active subreddits
-- this has been updated with more correct count data
--


drop table if exists submissions_monthly_engagement2;

with 
--subreddit_list as (
--	select name from subreddit_summary
--	where total_activity >= 1000 and unique_authors >= 10
--),
subreddit_list as (
	select name from subreddit_summary
	where total_activity >= 1000 and unique_authors >= 10
)

select
	subreddit,
	date_trunc('month', ser.created_utc) as month,
	
	count(id) filter(where score>1) as gt_1,
	count(id) filter(where score>0) as gt_0,
	count(id) filter(where score=1) as eq_1,
	count(id) filter(where score=0) as eq_0,
	count(id) filter(where score<1) as lt_1,
	count(id) filter(where comment_count_from_data>0) as has_comments,
	count(id) filter(where comment_count_from_data > 0 and comment_count_from_data = automod_comment_count) as count_posts_with_only_automod_comments,
	count(id) filter(where score>1 or (score = 1 and comment_count_from_data>0)) as num_pos_engaged,
	count(id) filter(where score>1 or (score = 1 and comment_count_from_data>0 and comment_count_from_data != automod_comment_count)) as num_pos_engaged_no_automod,
	count(id) filter(where score!=1 or comment_count_from_data>0) as num_any_engaged,
	count(id) filter(where score!=1 or (comment_count_from_data>0 and comment_count_from_data = automod_comment_count)) as num_any_engaged_exclude_automod,
	count(id) filter(where score>1 or comment_count_from_data>0) as num_engaged,
	count(id) filter(where score>0 or (comment_count_from_data>0 and score>0)) as num_engaged_sc_gt0,
	count(*) as total_submissions,
	sum(comment_count_from_data) as total_comment_counts_from_data,
	sum(num_comments_in_json) as total_comments_from_submission_json
	
into table submissions_monthly_engagement2
from subreddit_list sl
left join submission_engagement_raw ser on ser.subreddit = sl.name
group by subreddit, month
order by subreddit, month asc;



create index on submissions_monthly_engagement2(subreddit);
create index on submissions_monthly_engagement2(month);

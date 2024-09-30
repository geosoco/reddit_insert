--
-- s2_submissions_engagement_monthly
--
-- rough engagement estimates of submission data across active subreddits
-- this has been updated with more correct count data
--


drop table if exists s2_submissions_engagement_monthly;


with subreddit_list as (
	select display_name as name, created_utc
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' 
)

select
	subreddit,
	(extract(epoch from (ser.created_utc - sl.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
	
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
	
into table s2_submissions_engagement_monthly
from subreddit_list sl
left join submission_engagement_raw ser on ser.subreddit = sl.name
group by subreddit, creation_delta_months
order by subreddit, creation_delta_months asc;



create index on s2_submissions_engagement_monthly(subreddit);
create index on s2_submissions_engagement_monthly(creation_delta_months);


grant select on s2_submissions_engagement_monthly to public;

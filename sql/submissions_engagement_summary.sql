--
-- submissions_engagement_summary
--
-- rough engagement estimates of submission data across active subreddits
-- this has been updated with more correct count data
--


drop table if exists submissions_engagement_summary;

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
	sl.name,
	
	sum(total_submissions) as total_submissions,

	sum(gt_1) as score_gt1,
	sum(gt_0) as score_gt0,
	sum(eq_0) as score_eq0,
	sum(eq_1) as score_eq1,
	sum(lt_1) as score_lt1,
	sum(has_comments) as total_has_comments,
	sum(count_posts_with_only_automod_comments) as total_posts_with_only_automod_comments,
	sum(num_pos_engaged) as total_pos_engaged,
	sum(num_pos_engaged_no_automod) as total_pos_engaged_no_automod,
	sum(num_any_engaged) as total_any_engaged,
	sum(num_any_engaged_exclude_automod) as total_any_engagement_excluding_automod,
--	num_engaged (this is a mistake)
	sum(num_engaged_sc_gt0) as total_engaged_sc_gt0,
	sum(total_comment_counts_from_data) as total_comment_counts_from_data,
	sum(total_comments_from_submission_json) as total_comments_from_submission_json,

	sum(num_pos_engaged) * 100.0 / sum(total_submissions) as pct_pos_engaged,
	sum(num_any_engaged) * 100.0 / sum(total_submissions) as pct_any_engaged

into table submissions_engagement_summary
from subreddit_list sl
left join submissions_monthly_engagement2 sme on sme.subreddit = sl.name
group by subreddit;


grant select on submissions_engagement_summary to public;

create index on submissions_engagement_summary(subreddit);


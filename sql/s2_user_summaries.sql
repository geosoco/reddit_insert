--
--
-- s2_user_summaries
--
--
-- A smaller table of users from the s2 study, pre-sorted by activity in order to pilot some bot-detection of bot subs
--
-- This table takes a long time to generate as the source table is one of the largest in the database
--


drop table if exists s2_user_summaries;


with active_comment_first_months as (
  select s2.subreddit, 
    count(distinct creation_delta_months) as active_months
    from s2_subreddit_30day_activity_summary s2
    where s2.total_comments > 0 and s2.creation_delta_months < 6  
    group by subreddit
),
eligible_subs as (
select 
  acfm.subreddit, ss.total_activity as total_sub_activity
  
	from active_comment_first_months acfm
  left join subreddit_summary  ss on acfm.subreddit = ss.name
	where ss.created_utc >= '2012-01-01' and ss.created_utc < '2013-01-01' and  acfm.active_months > 3 and ss.total_submissions >= 100 and ss.total_comments >= 100 and ss.unique_authors >= 10
)
select
	usa.*
into s2_user_summaries
from eligible_subs es
left join user_subreddit_activity usa on usa.subreddit = es.subreddit
order by subreddit, total_activity desc;





create index on s2_user_summaries(subreddit);
create index on s2_user_summaries(author);

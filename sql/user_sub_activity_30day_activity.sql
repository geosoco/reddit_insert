--
-- user_sub_activity_30day_activity
--
-- This table is used to create the retention/turnover table
-- There are some intermediate analyses that could use this data, but 
-- otherwise could be a temp table just to create turnover table
--

drop table if exists user_sub_activity_30day_activity;

select
	uca.subreddit,
	uca.author, 
	(extract(epoch from (uca.created_utc - s.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
	count(*) as total_activity,
	sum(case when c_id is not null then 1 else 0 end) as total_comments,
	sum(case when s_id is not null then 1 else 0 end) as total_submissions

into user_sub_activity_30day_activity
from user_combined_activity uca
left join subreddits s on s.display_name = uca.subreddit
group by uca.subreddit, uca.author, ((extract(epoch from (uca.created_utc - s.created_utc))::bigint) / (3600*24*30)::int)
--where subreddit in ('Eskrima', 'AskWomenOver30') and uca.created_utc >= '2015-01-01' and uca.created_utc < '2015-03-01'


grant select on user_sub_activity_30day_activity to public;
create index on user_sub_activity_30day_activity(subreddit);
create index on user_sub_activity_30day_activity(author);


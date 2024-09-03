-- subreddit_30day_retention
--
-- NOTE: This isn't calculated properly. It should be dividng by the total from the previous month
-- it is fixed in subreddit_30day_retention_fixed.sql
-- 

drop table if exists subreddit_30day_retention;


with lead_data as (
select
	author,
	subreddit, 
	creation_delta_months,
	lag(creation_delta_months) over (partition by subreddit, author order by creation_delta_months asc) as prev_val,
	total_activity,
	total_submissions,
	total_comments
from user_sub_activity_30day_activity
where author != '[deleted]'
)

select
	subreddit, creation_delta_months, count(*) as total_active, 
		sum(case when creation_delta_months-1 = prev_val then 1 else 0 end) as active_prev_month,
		(sum(case when creation_delta_months-1 = prev_val then 1 else 0 end) * 100.0 / count(*)) as retention_rate,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
into subreddit_30day_retention		
from lead_data
group by subreddit, creation_delta_months
order by subreddit, creation_delta_months;


grant select on subreddit_30day_retention to public;

create index on subreddit_30day_retention(subreddit);


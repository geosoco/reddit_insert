--
-- subreddit_30day_retention_fixed
--


drop table if exists subreddit_30day_retention_fixed;


with lead_data as (
select
	author,
	subreddit, 
	creation_delta_months,
	lead(creation_delta_months) over (partition by subreddit, author order by creation_delta_months asc) as next_active_month,
	case when lead(creation_delta_months) over (partition by subreddit, author order by creation_delta_months asc) = creation_delta_months+1 then 1 else 0 end as active_next_month,
	total_activity,
	total_submissions,
	total_comments
from user_sub_activity_30day_activity
where author != '[deleted]'
)

select
	subreddit, creation_delta_months, 
	count(*) as total_active_authors, 
	sum(active_next_month) as active_next_month,
	(sum(case when creation_delta_months+1 = next_active_month then 1 else 0 end) * 100.0 / count(*)) as retention_rate,
	100.0 - (sum(case when creation_delta_months+1 = next_active_month then 1 else 0 end) * 100.0 / count(*)) as turnover_rate,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
into subreddit_30day_retention_fixed		
from lead_data
group by subreddit, creation_delta_months
order by subreddit, creation_delta_months;


grant select on subreddit_30day_retention_fixed to public;

create index on subreddit_30day_retention_fixed(subreddit);


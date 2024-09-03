drop table if exists s1_subreddit_30day_retention_fixed2;

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
and subreddit in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
), 
subreddit_totals as (
select
	subreddit, creation_delta_months, 
	count(*) as total_active_authors, 
	sum(active_next_month) as active_next_month,
	(sum(case when creation_delta_months+1 = next_active_month then 1 else 0 end) * 100.0 / count(*)) as retention_rate,
	100.0 - (sum(case when creation_delta_months+1 = next_active_month then 1 else 0 end) * 100.0 / count(*)) as turnover_rate,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
from lead_data
group by subreddit, creation_delta_months
order by subreddit, creation_delta_months
)
select
	subreddit, creation_delta_months, total_active_authors, 
	active_next_month as active_authors_next_month, 
	retention_rate as retention_rate_next_month,
	turnover_rate as turnover_rate_next_month,
	total_activity, 
	total_submissions,
	total_comments,
	
	case when lag(creation_delta_months) over (partition by subreddit order by creation_delta_months asc) IS NOT NULL 
		and lag(creation_delta_months) over (partition by subreddit order by creation_delta_months asc) = creation_delta_months-1 
	then lag(retention_rate) over (partition by subreddit order by creation_delta_months asc)
	else 0
	end as retention_rate,
	
	case when lag(creation_delta_months) over (partition by subreddit order by creation_delta_months asc) IS NOT NULL 
		and lag(creation_delta_months) over (partition by subreddit order by creation_delta_months asc) = creation_delta_months-1 
	then lag(turnover_rate) over (partition by subreddit order by creation_delta_months asc)
	else 0
	end as turnover_rate
into s1_subreddit_30day_retention_fixed2
from subreddit_totals
order by subreddit, creation_delta_months


grant  select on s1_subreddit_30day_retention_fixed2 to public;
	
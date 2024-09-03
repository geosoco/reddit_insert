--
-- WARNING THIS DATA IS CALCULATED INCORRECTLY. It divides the # retained users for t-1 by the total users in t0
-- This has been corrected in s1_subreddit_retention_fixed2.sql
--

drop table if exists s1_subreddit_30day_retention_fixed;

with lead_data as (
select
	author,
	subreddit, 
	creation_delta_months,
	lead(creation_delta_months) over (partition by subreddit, author order by creation_delta_months asc) as next_val,
	total_activity,
	total_submissions,
	total_comments
from user_sub_activity_30day_activity
where author != '[deleted]'
and subreddit in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
)

select
	subreddit, creation_delta_months as creation_delta_months, count(*) as total_active, 
		case when creation_delta_months+1 = next_val then 1 else 0 end as active_next_month,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
into s1_subreddit_30day_retention_fixed
from lead_data
group by subreddit, creation_delta_months
order by subreddit, creation_delta_months;



		(sum(case when creation_delta_months+1 = next_val then 1 else 0 end) * 100.0 / count(*)) as retention_rate,


grant select on s1_subreddit_30day_retention_fixed to public;

create index on s1_subreddit_30day_retention_fixed(subreddit);



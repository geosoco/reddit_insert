--
-- s2_last_3month_activity_2years
--
--
-- This is an aggregated metric of success from the Cunha paper which looks 2 years out and the final 3 months of that
--
--



drop table if exists s2_last_3month_activity_2years;

with last_activity as (
select
	subreddit, 
	count(distinct author) filter (where creation_delta_months > 20) as last_3_months,
	count(distinct author) as total_unique_authors
from s2_user_subreddit_activity_30day
where creation_delta_months < 24
group by subreddit
)

select
	la.*,
	last_3_months::decimal / total_unique_authors::decimal as pct_active_last_3mo
into s2_last_3month_activity_2years
from last_activity la;


grant select on s2_last_3month_activity_2years to public;

create index on s2_last_3month_activity_2years(subreddit);
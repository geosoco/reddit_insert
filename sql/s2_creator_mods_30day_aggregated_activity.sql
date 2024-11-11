--
-- s2_creator_mods_30day_aggregated_activity
--
--
--
--
--


drop table if exists s2_creator_mods_30day_aggregated_activity;


select
	creation_delta_months,
	account_type,
	count(distinct subreddit) as num_subs,
	count(distinct account) as num_accounts,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments

into s2_creator_mods_30day_aggregated_activity
from s2_creator_mods_30day_activity_all_users

group by creation_delta_months, account_type
order by creation_delta_months, account_type;


grant select on s2_creator_mods_30day_aggregated_activity to public;
--
-- s2_30day_active_mac_subreddit_counts
--
--
-- This is a summary table to speed up queries
--
--

drop table if exists s2_30day_active_mac_subreddit_counts;

with eligible_subs as (
select
		subreddit, 
		count(distinct case when is_creator = 1 and account != '[deleted]' then account else null end) as num_creators, 
		count(distinct case when is_mod = 1  and account != '[deleted]' then account else null end) as num_mods 
	from s2_mods_and_creators
	group by subreddit
),
valid_subs as (
select
	distinct subreddit
from eligible_subs 
where num_creators > 0 and num_mods > 0
),
combined_data as (
	select 
		usa.*
		
	from valid_subs vs
	left join s2_user_subreddit_activity_30day usa on vs.subreddit = usa.subreddit

)
select creation_delta_months, count(distinct subreddit)
into s2_30day_active_mac_subreddit_counts
from combined_data
group by creation_delta_months;

grant select on s2_30day_active_mac_subreddit_counts to public;

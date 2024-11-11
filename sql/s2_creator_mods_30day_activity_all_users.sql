--
-- s2_creator_mods_30day_activity_all_users
--
-- This table contains 30day period activity for all subreddits for which I could collect creator and moderator data
--
--
--
--
--
--
--



drop table if exists s2_creator_mods_30day_activity_all_users;



with mac_subs as (
	select
		distinct subreddit
	from s2_mods_and_creators
),
mods_and_creators as (
	select
		subreddit, account, 
		is_creator,
		is_mod
	from s2_mods_and_creators
	where account != '[deleted]'
),
sub_activity as (
select
	ms.subreddit as sub1,
	usa.subreddit as sub2,
	coalesce(account, author) as account,
	case when is_creator = 1 then 'creator'
		when is_mod = 1 then 'mod' 
		else 'normal' 
		end as account_type,
	creation_delta_months,
	total_activity,
	num_comments as total_comments,
	num_submissions as total_submissions
from mac_subs ms
left join s2_user_subreddit_activity_30day usa on usa.subreddit = ms.subreddit
left join mods_and_creators mac on mac.subreddit = ms.subreddit and usa.author = mac.account
)
select *
into s2_creator_mods_30day_activity_all_users
from sub_activity;


grant select on s2_creator_mods_30day_activity_all_users to public;

create index on s2_creator_mods_30day_activity_all_users(subreddit);
create index on s2_creator_mods_30day_activity_all_users(account);
create index on s2_creator_mods_30day_activity_all_users(subreddit, account);









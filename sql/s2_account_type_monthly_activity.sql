--
-- s2_account_type_monthly_activity
--
--
--
--
--


with mac_subs as (
	select
		distinct subreddit
	from s2_mods_and_creators
	where account != '[deleted]'
),
creators as (
	select distinct subreddit, creator
	from subreddit_creator_updates
	where creator != '[deleted]'
),
moderators as (
	select distinct subreddit, moderator
	from subreddit_moderator_updates
	where moderator != '[deleted]'
),
mods_and_creator_subs as (
	select subreddit, sum(a.num_mods), sum(a.num_creators), bool_or(has_mods) as has_moderator, bool_or(has_creator) as has_creator
	from (
		select subreddit, count(distinct case when moderator != '[deleted]' then moderator else NULL end) as num_mods, 0 as num_creators,
			bool_or(moderator is not NULL) as has_mods, TRUE as has_creator
		from subreddit_moderator_updates  group by subreddit
	
		union all
	
		select subreddit, 0 as num_mods, count(distinct case when creator != '[deleted]' then creator else null end) as num_creators, 
		FALSE as has_mods, bool_or(creator is not null) as has_creator
		from subreddit_creator_updates group by subreddit
	) a
	group by subreddit
),
combined_data as (
	select 
		usa.*,
		case when m.moderator is not null then 1 else 0 end as is_mod,
		case when c.creator is not null then 1 else 0 end as is_creator,
		case when m.moderator is not null or c.creator is not null then 1 else 0 end as is_mod_or_creator,
		case when c.creator is not null then 'creator'
		when m.moderator is not null then 'mod'
		else 'normal' end as account_type
		
	from mods_and_creator_subs macs
	left join s2_user_subreddit_activity_30day usa on macs.subreddit = usa.subreddit
	left join moderators m on m.subreddit = usa.subreddit and m.moderator = usa.author
	left join creators c on c.subreddit = usa.subreddit and c.creator = usa.author

),
aggregated_data as (
	select
		creation_delta_months,
		account_type,
		count(distinct author) as num_accounts,
		sum(total_activity) as total_activity,
		sum(num_submissions) as total_submissions,
		sum(num_comments) as total_comments
	from combined_data
	group by creation_delta_months, account_type
)
select
	*
into s2_account_type_monthly_activity
from aggregated_data;

grant select on s2_account_type_monthly_activity to public;
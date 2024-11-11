--
-- s2_mods_and_creators
--
--
--
--
--
--



drop table if exists s2_mods_and_creators;

with creators as (
	select distinct subreddit, creator as account, 'creator' as type
	from subreddit_creator_updates
),
moderators as (
	select distinct subreddit, moderator as account, 'moderator' as type
	from subreddit_moderator_updates
)
select 
	subreddit, account, 
	bit_or(case when type = 'moderator' then 1 else 0 end) as is_mod, 
	bit_or(case when type = 'creator' then 1 else 0 end) as is_creator,
	bit_or(case when type = 'moderator' or type = 'creator' then 1 else 0 end) as is_mod_or_creator

	into s2_mods_and_creators
	
	from (
		select subreddit, account, type
		from creators c
	
		union all
	
		select subreddit, account, type
		from moderators m
	) a
	group by subreddit, account;



grant select on s2_mods_and_creators to public;


create index on s2_mods_and_creators(subreddit);
create index on s2_mods_and_creators(account);
create index on s2_mods_and_creators(subreddit, account);
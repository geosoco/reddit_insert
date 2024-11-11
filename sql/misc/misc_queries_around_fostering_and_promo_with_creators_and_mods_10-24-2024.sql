--
--
--
--
--

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
	mac.subreddit,
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
select
	creation_delta_months,
	account_type,
	count(distinct subreddit) as total_subreddits,
	count(distinct account) as total_authors,
	sum(total_activity) as total_activity,
	sum(total_comments) as total_comments,
	sum(total_submissions) as total_submissions
from sub_activity
group by creation_delta_months, account_type
order by creation_delta_months, account_type









--
--
--
--
--

with mac_subs as (
	select
		distinct subreddit
	from s2_mods_and_creators
),
mods_and_creators as (
	select
		subreddit, account, is_mod, is_creator, is_mod_or_creator
	from s2_mods_and_creators
	where account != '[deleted]'
),
sub_foster_data as (
	select
		susd.subreddit, 
		suri.author,
		coalesce(macs.is_mod, 0) as is_mod,
		coalesce(macs.is_creator, 0) as is_creator,
		coalesce(macs.is_mod_or_creator, 0) as is_power_user,
		suri.creation_delta_months,
		suri.total_activity,
		suri.total_submissions,
		suri.total_comments,
		susd.first_delta_month,
		susd.last_delta_month
	from mac_subs ms
	left join mods_and_creators macs on ms.subreddit = macs.subreddit
	full join s2_sub_user_sequence_data susd on ms.subreddit = susd.subreddit and macs.account = susd.author
	left join s2_sub_user_retention_intermediate suri on suri.subreddit = susd.subreddit and suri.author = susd.author and suri.creation_delta_months >= susd.first_delta_month and suri.creation_delta_months <= susd.last_delta_month
	where susd.total_months >= 3 and susd.total_submissions > (total_months * 10)  and susd.author != '[deleted]'
),
aggregated as (
	select
		subreddit,
		--is_power_user,
		is_creator as is_power_user,
		creation_delta_months,
		count(distinct author) as num_fostering_accounts, 
		sum(total_activity) as total_fostering_activity, 
		sum(total_submissions) as total_fostering_submissions, 
		sum(total_comments) as total_fostering_comments
	from sub_foster_data
	group by subreddit, is_creator, creation_delta_months
)
select 
ms.subreddit, s30.creation_delta_months, is_power_user, 
s30.total_activity, s30.total_submissions, s30.total_comments,
unique_authors, afd.num_fostering_accounts, 
afd.total_fostering_activity, afd.total_fostering_submissions, afd.total_fostering_comments
from mac_subs ms
left join s2_subreddit_30day_activity_summary s30 on s30.subreddit = ms.subreddit
left join aggregated afd on afd.subreddit = s30.subreddit and afd.creation_delta_months = s30.creation_delta_months







--
--
--
--
--


with mac_subs as (
	select
		distinct subreddit
	from s2_mods_and_creators
),
mods_and_creators as (
	select
		subreddit, account, is_mod, is_creator, is_mod_or_creator
	from s2_mods_and_creators
	where account != '[deleted]'
),
sub_foster_data as (
	select
		susd.subreddit, 
		suri.author,
		coalesce(macs.is_mod, 0) as is_mod,
		coalesce(macs.is_creator, 0) as is_creator,
		coalesce(macs.is_mod_or_creator, 0) as is_power_user,
		suri.creation_delta_months,
		suri.total_activity,
		suri.total_submissions,
		suri.total_comments,
		susd.first_delta_month,
		susd.last_delta_month
	from mac_subs ms
	left join mods_and_creators macs on ms.subreddit = macs.subreddit
	full join s2_sub_user_sequence_data susd on ms.subreddit = susd.subreddit and macs.account = susd.author
	left join s2_sub_user_retention_intermediate suri on suri.subreddit = susd.subreddit and suri.author = susd.author and suri.creation_delta_months >= susd.first_delta_month and suri.creation_delta_months <= susd.last_delta_month
	where susd.total_months >= 3 and susd.total_submissions > (total_months * 10)  and susd.author != '[deleted]'
),
aggregated as (
	select
		subreddit,
		--is_power_user,
		is_creator as is_power_user,
		creation_delta_months,
		count(distinct author) as num_fostering_accounts, 
		sum(total_activity) as total_fostering_activity, 
		sum(total_submissions) as total_fostering_submissions, 
		sum(total_comments) as total_fostering_comments
	from sub_foster_data
	group by subreddit, is_creator, creation_delta_months
)
select 
ms.subreddit, s30.creation_delta_months, is_power_user, 
s30.total_activity, s30.total_submissions, s30.total_comments,
unique_authors, afd.num_fostering_accounts, 
afd.total_fostering_activity, afd.total_fostering_submissions, afd.total_fostering_comments
from mac_subs ms
left join s2_subreddit_30day_activity_summary s30 on s30.subreddit = ms.subreddit
left join aggregated afd on afd.subreddit = s30.subreddit and afd.creation_delta_months = s30.creation_delta_months






--
--
--
--
--


with mods_and_creators as (
	select
		*
	from s2_mods_and_creators
	where account != '[deleted]'
),
combined_data as (
	select 
		aasa.*,
		coalesce(mac.is_mod, 0) as is_mod,
		coalesce(mac.is_creator, 0) as is_creator,
		coalesce(mac.is_mod_or_creator, 0) as is_mod_or_creator
	from s2_adv_author_sub_activity aasa
	left join mods_and_creators mac on lower(mac.subreddit) = lower(aasa.mentioned_sub_name) and mac.account = aasa.author

)
select
	is_mod_or_creator, count(distinct author), 
	sum(num_comment_refs) as num_comment_refs,  sum(num_comment_links) as num_comment_links, sum(num_comment_mentions) as num_comment_mentions, 
	sum(num_submission_refs) as num_submission_refs
from combined_data
group by is_mod_or_creator




--select count(distinct subreddit), count(distinct account) from s2_mods_and_creators






--
--
--
--
--

with creators as (
	select distinct subreddit, creator as account, 'creator' as type
	from subreddit_creator_updates
	where creator != '[deleted]'
),
moderators as (
	select distinct subreddit, moderator as account, 'moderator' as type
	from subreddit_moderator_updates
	where moderator != '[deleted]'
),
mods_and_creators as (
select 
	subreddit, account, 
	bit_or(case when type = 'moderator' then 1 else 0 end) as is_mod, 
	bit_or(case when type = 'creator' then 1 else 0 end) as is_creator
	from (
		select subreddit, account, type
		from creators c
	
		union all
	
		select subreddit, account, type
		from moderators m
	) a

	group by subreddit, account
)
,
combined_data as (
	select 
		aasa.*,
		coalesce(mac.is_mod, 0) as is_mod,
		coalesce(mac.is_creator, 0) as is_creator,
		case when mac.is_mod = 1 or mac.is_creator = 1 then 1 else 0 end  as is_mod_or_creator
	from s2_adv_author_sub_activity aasa
	left join mods_and_creators mac on mac.subreddit = aasa.mentioned_sub_name and mac.account = aasa.author
)
select
	is_mod_or_creator, count(distinct author), sum(num_comment_links), sum(num_comment_mentions)
from combined_data
group by is_mod_or_creator;

-- select 
-- 	mac1.subreddit, mac1.account, mac2.subreddit, mac2.account
-- from mods_and_creators mac1
-- full outer join s2_mods_and_creators mac2 on mac2.subreddit = mac1.subreddit and mac2.account = mac1.account
-- where mac2.account != '[deleted]' and (mac2.account is null or mac1.account is null)







--
--
--
--
--

with creators as (
	select distinct subreddit, creator as account, 'creator' as type
	from subreddit_creator_updates
	where creator != '[deleted]'
),
moderators as (
	select distinct subreddit, moderator as account, 'moderator' as type
	from subreddit_moderator_updates
	where moderator != '[deleted]'
),
mods_and_creators as (
select 
	subreddit, account, 
	bit_or(case when type = 'moderator' then 1 else 0 end) as is_mod, 
	bit_or(case when type = 'creator' then 1 else 0 end) as is_creator
	from (
		select subreddit, account, type
		from creators c
	
		union all
	
		select subreddit, account, type
		from moderators m
	) a

	group by subreddit, account
)
-- combined_data as (
-- 	select 
-- 		aasa.*,
-- 		coalesce(mac.is_mod, 0) as is_mod,
-- 		coalesce(mac.is_creator, 0) as is_creator,
-- 		case when mac.is_mod = 1 or mac.is_creator = 1 then 1 else 0 end  as is_mod_or_creator
-- 	from s2_adv_author_sub_activity aasa
-- 	left join mods_and_creators mac on mac.subreddit = aasa.mentioned_sub_name and mac.account = aasa.author
-- )
select
	mac.subreddit,
	mac.account,
	csld.*
--	count(*) as cnt
	
from mods_and_creators mac
left join s2_comment_sub_link_details csld on csld.mentioned_sub_name = lower(mac.subreddit) and csld.author = mac.account
where id is not null and link_type = 'link'
limit 10000;
--group by mac.account
--order by cnt desc;

-- advertising as (
-- 	select * 
-- 	from s2_adv_author_sub_activity

-- )









--
--
--
--
--


with creators as (
	select distinct subreddit, creator as account, 'creator' as type
	from subreddit_creator_updates
	where creator != '[deleted]'
),
moderators as (
	select distinct subreddit, moderator as account, 'moderator' as type
	from subreddit_moderator_updates
	where moderator != '[deleted]'
),
mods_and_creators as (
select 
	subreddit, account, 
	bit_or(case when type = 'moderator' then 1 else 0 end) as is_mod, 
	bit_or(case when type = 'creator' then 1 else 0 end) as is_creator
	from (
		select subreddit, account, type
		from creators c
	
		union all
	
		select subreddit, account, type
		from moderators m
	) a

	group by subreddit, account
)
-- combined_data as (
-- 	select 
-- 		aasa.*,
-- 		coalesce(mac.is_mod, 0) as is_mod,
-- 		coalesce(mac.is_creator, 0) as is_creator,
-- 		case when mac.is_mod = 1 or mac.is_creator = 1 then 1 else 0 end  as is_mod_or_creator
-- 	from s2_adv_author_sub_activity aasa
-- 	left join mods_and_creators mac on mac.subreddit = aasa.mentioned_sub_name and mac.account = aasa.author
-- )
select
	mac.subreddit,
	mac.account,
	count(*) as cnt
	
from mods_and_creators mac
left join s2_comment_sub_link_details csld on lower(csld.mentioned_sub_name) = mac.subreddit and csld.author = mac.account


-- advertising as (
-- 	select * 
-- 	from s2_adv_author_sub_activity

-- )








--
--
--
--
--


with year_subs as (
	select 
		display_name, 
		created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
),

comment_ads as (
	select
		ys.display_name as mentioned_sub_name, author, count(distinct id) as count_comment_refs

		count(distinct case when link_type = 'link' then id else NULL end) as num_link_comments,
		count(distinct case when link_type = 'mention' then id else NULL end) as num_mention_comments,
	
		
	from year_subs ys
	left join s2_comment_sub_link_details sld on sld.mentioned_sub_name = lower(ys.display_name)
	where self_reference != TRUE 
	group by ys.display_name, author
),
submission_links as (
select
	ys.display_name as mentioned_sub_name,
	a.author,
	count(distinct stsld.id) as num_submission_links
	count(distinct sssld.id) as num_

	a.author, 
	a.id,
	a.mentioned_sub_name,
	(extract(epoch from (a.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
	coalesce(stsld.subreddit, sssld.subreddit) as subreddit,
	coalesce(stsld.created_utc, sssld.created_utc) as created_utc,
	coalesce(stsld.author, sssld.author) as author,

	coalesce(stsld.unique_links_count,0) as title_unique_links_count,
	coalesce(sssld.unique_links_count,0) as selftext_unique_links_count,

	stsld.link_type as title_link_type,
	sssld.link_type as selftext_link_type
	
	from year_subs ys
	left join ( 
		select
			distinct stsld.id, ys.display_name as mentioned_sub_name, stsld.created_utc
		from year_subs ys 
		left join s2_submission_title_sub_link_details stsld on lower(ys.display_name) = stsld.mentioned_sub_name
		union all
		select
			distinct sssld.id, ys.display_name as mentioned_sub_name, sssld.created_utc
		from year_subs ys 
		left join s2_submission_selftext_sub_link_details sssld on lower(ys.display_name) = sssld.mentioned_sub_name
	) a	on a.mentioned_sub_name = ys.display_name
	left join s2_submission_title_sub_link_details stsld on stsld.id = a.id
	left join s2_submission_selftext_sub_link_details sssld on sssld.id = a.id
),
submissions_summarized as (
	select
		sm.subreddit,
		sm.creation_delta_months,
		count(distinct sl.subreddit) as total_subreddits,
		count(distinct sl.id) as total_submissions,
		count(distinct sl.author) as total_authors,
		count(case when author != '[deleted]' then author else NULL end) as non_deleted_authors,
	
		count(distinct case when title_unique_links_count > 0 then sl.id else NULL end) as distinct_title_submissions,
		count(distinct case when selftext_unique_links_count > 0 then sl.id else NULL end) as distinct_selftext_submissions,
		
		count(distinct case when title_unique_links_count > 0 then sl.subreddit else NULL end) as distinct_title_subreddits,
		count(distinct case when selftext_unique_links_count > 0 then sl.subreddit else NULL end) as distinct_selftext_subreddits,
	
		count(distinct case when title_unique_links_count > 0 then author else NULL end) as distinct_title_authors,
		count(distinct case when selftext_unique_links_count > 0 then author else NULL end) as distinct_selftext_authors,
	
		count(case when title_unique_links_count > 0 and title_link_type = 'link' then sl.id else 0 end ) as total_title_link_submissions,
		count(case when title_unique_links_count > 0 and title_link_type = 'mention' then sl.id else 0 end ) as total_title_mention_submissions,
		count(case when selftext_unique_links_count > 0 and title_link_type = 'link' then sl.id else 0 end) as total_selftext_link_submissions,
		count(case when selftext_unique_links_count > 0 and title_link_type = 'mention' then sl.id else 0 end) as total_selftext_mention_submissions,	
		
		sum(case when title_unique_links_count > 0 then 1 else 0 end ) as total_title_links_and_mentions,
		sum(case when selftext_unique_links_count > 0 then 1 else 0 end) as total_selftext_links_and_mentions,
	
		sum(case when title_unique_links_count > 0 and title_link_type = 'link' then 1 else 0 end ) as total_title_links,
		sum(case when title_unique_links_count > 0 and title_link_type = 'mention' then 1 else 0 end ) as total_title_mentions,
		sum(case when selftext_unique_links_count > 0 and title_link_type = 'link' then 1 else 0 end) as total_selftext_links,
		sum(case when selftext_unique_links_count > 0 and title_link_type = 'mention' then 1 else 0 end) as total_selftext_mentions
	
		
		
	from sub_months sm
	left join submission_links sl on sm.subreddit = sl.mentioned_sub_Name and sm.creation_delta_months = sl.creation_delta_months
	group by sm.subreddit, sm.creation_delta_months
),
cross_posts as (
	select
		sm.subreddit,
		sm.creation_delta_months,
		coalesce(cps.num_authors, 0) as num_crosspost_authors,
		coalesce(cps.non_deleted_authors, 0) as num_crosspost_nondeleted_authors,
		coalesce(cps.num_subreddits, 0) as num_crosspost_subreddits,
		coalesce(cps.total_cross_posts, 0) as num_crosspost_submissions
	from sub_months sm
	left join s2_cross_posts_30day_summary cps on cps.mentioned_sub_name = sm.subreddit and cps.creation_delta_months = sm.creation_delta_months
	
)






--
--
--
--
--

with creators as (
	select distinct subreddit, creator
	from subreddit_creator_updates
	where creator != '[deleted]'
),
moderators as (
	select distinct subreddit, moderator
	from subreddit_moderator_updates
	where moderator != '[deleted]'
),
mods as (
	select subreddit, count(distinct moderator) as count_moderators
	from subreddit_moderator_updates
	where moderator != '[deleted]' and moderator is not null
	group by subreddit
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
sub_foster_data as (
	select
		susd.subreddit, 
		susd.author,
		count(distinct seq_id) as num_seqs,
		sum(susd.total_months) as total_months,
		sum(total_activity) as total_activity,
		sum(total_comments) as total_comments,
		sum(total_submissions) as total_submissions
	
	from mods_and_creator_subs macs
	left join s2_sub_user_sequence_data susd on macs.subreddit = susd.subreddit
	where susd.total_months >= 3 and susd.total_submissions > (total_months * 10)  and susd.author != '[deleted]'
	
	group by susd.subreddit, susd.author

)

select count(distinct subreddit)
from sub_foster_data sfd








--
--
--
--
--


with creators as (
	select distinct subreddit, creator
	from subreddit_creator_updates
	where creator != '[deleted]'
),
moderators as (
	select distinct subreddit, moderator
	from subreddit_moderator_updates
	where moderator != '[deleted]'
),
mods as (
	select subreddit, count(distinct moderator) as count_moderators
	from subreddit_moderator_updates
	where moderator != '[deleted]' and moderator is not null
	group by subreddit
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
sub_foster_data as (
	select
		susd.subreddit, 
		susd.author,
		count(distinct seq_id) as num_seqs,
		sum(susd.total_months) as total_months,
		sum(total_activity) as total_activity,
		sum(total_comments) as total_comments,
		sum(total_submissions) as total_submissions
	
	from mods_and_creator_subs macs
	left join s2_sub_user_sequence_data susd on macs.subreddit = susd.subreddit
	where susd.total_months >= 3 and susd.total_submissions > (total_months * 10)  and susd.author != '[deleted]'
	
	group by susd.subreddit, susd.author

)



	select
		macs.subreddit,
		coalesce(sfd.author, c.creator, m.moderator) as author,
		sfd.num_seqs,
		sfd.total_months,
		sfd.total_activity,
		sfd.total_comments,
		sfd.total_submissions,
		case when c.creator is not null then 1 else 0 end as is_creator,
		case when m.moderator is not null then 1 else 0 end as is_mod
		
	from mods_and_creator_subs macs
	left join sub_foster_data sfd on macs.subreddit = sfd.subreddit
	left join creators c on c.subreddit = sfd.subreddit and c.creator = sfd.author
	left join moderators m on m.subreddit = sfd.subreddit and m.moderator = sfd.author	
	







--
--
--
--
--


with eligible_subreddits as (
	select
		name as subreddit,
		ss.created_utc as created_utc,
		floor(extract(epoch from ('2017-01-01 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddit_summary ss
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' 
	and ss.total_comments > 25 and ss.unique_authors >= 10
),
monthly_activity as (
	select
		es.subreddit,
		min(sas.creation_delta_months) as first_month,
		count(distinct creation_delta_months) as active_months
	from eligible_subreddits es
	left join s2_subreddit_30day_activity_summary sas on sas.subreddit = es.subreddit
	where sas.total_activity > 0
	group by es.subreddit
),

subs as (
select
	ma.first_month as first_active_month,
	ma.active_months as num_active_months,
	sd.missing_start,
	sd.total_missing_months,
	sd.start_period_date,
	sd.end_period_date,
	sd.death,
	(case when sd.missing_start is not null then sd.missing_start else es.max_months end) - creation_delta_months  as remaining_lifetime,
	smdc.*
from eligible_subreddits es
left join monthly_activity ma on ma.subreddit = es.subreddit
left join s2_subreddit_monthly_data_combined smdc on es.subreddit = smdc.subreddit
left join s2_subreddit_deaths sd on es.subreddit = sd.subreddit

where 
(sd.missing_start is null or smdc.creation_delta_months <= sd.missing_start) and first_month = 0 
)
select
	creation_delta_months, count(distinct subreddit)
from subs
group by creation_delta_months







--
--
--
--
--

with eligible_subreddits as (
	select
		name as subreddit,
		ss.created_utc as created_utc,
		floor(extract(epoch from ('2017-01-01 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddit_summary ss
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' 
	and ss.total_comments > 25 and ss.unique_authors >= 10
),
monthly_activity as (
	select
		es.subreddit,
		min(sas.creation_delta_months) as first_month,
		count(distinct creation_delta_months) as active_months
	from eligible_subreddits es
	left join s2_subreddit_30day_activity_summary sas on sas.subreddit = es.subreddit
	where sas.total_activity > 0
	group by es.subreddit
)


select
	ma.first_month as first_active_month,
	ma.active_months as num_active_months,
	sd.missing_start,
	sd.total_missing_months,
	sd.start_period_date,
	sd.end_period_date,
	sd.death,
	(case when sd.missing_start is not null then sd.missing_start else es.max_months end) - creation_delta_months  as remaining_lifetime,
	smdc.*
from eligible_subreddits es
left join monthly_activity ma on ma.subreddit = es.subreddit
left join s2_subreddit_monthly_data_combined smdc on es.subreddit = smdc.subreddit
left join s2_subreddit_deaths sd on es.subreddit = sd.subreddit

where 
sd.missing_start is null or smdc.creation_delta_months <= sd.missing_start
--	and sd.missing_start > 
	--and 
--	smdc.active_months >= 2
	--and 
--	smdc.lifetime_total_submissions >= 100 and smdc.lifetime_total_comments >= 25
--	 asdf
--and 
--sd.missing_start is null
--and 
--creation_delta_months = 0
--group by smdc.subreddit












--
--
--
--
--

with eligible_subreddits as (
	select
		name as subreddit,
		ss.created_utc as created_utc,
		ceil(extract(epoch from ('2016-12-31 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddit_summary ss
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' 
	and ss.total_comments > 25 and ss.unique_authors >= 10
	and ss.name = '0x10c'
)
select
	es.*,
	smdc.*

from 
	eligible_subreddits es
	left join s2_subreddit_monthly_data_combined smdc on es.subreddit = smdc.subreddit
--where subreddit in ('00101', '2Chainz', '210', '21conservative', '200Situps', '2092', '1000loantoday' )
--where subreddit in ('80sHipHop', '80sMetal', 'animecollections', 'animecraft')













--
--
--
--
--


with subreddit_ranges as (
	select
		name as subreddit,
		ss.created_utc as created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddit_summary ss
	left join s2_subreddit_30day_activity_summary sas on sas.subreddit = ss.name 
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' 
--	and ss.total_submissions >= 100 and ss.unique_authors >= 10 and ss.total_comments >= 10
	and sas.creation_delta_months = 0 
	and sas.total_activity > 0
	and name in ('0x01c', '0x10cships')
--	and name in ('2092', '20thcenturytrains', '210', '21conservative', '21crm', '2010sMusic', '200Situps', '1000loantoday' )
--and sas.total_comments is not null and sas.total_comments > 0
),
sub_months as (
select
	r.subreddit,
	r.created_utc,
	floor(r.max_months) as max_month,
	month
--	creation_delta_months,
--	lag(creation_delta_months) over w as prev_month,
--	creation_delta_months - lag(creation_delta_months) over w as missing_months,
--	total_comments

from subreddit_ranges r
cross join lateral generate_series(0,floor(r.max_months)::int) m(month)
--full outer join s2_subreddit_30day_activity_summary ssa on a.subreddit = ssa.subreddit and ssa.creation_delta_months = m.month
--where creation_delta_months < 12 
--window w as (partition by a.subreddit order by creation_delta_months asc)
order by r.subreddit, month
)
,
sub_month_borders as (
select
	a.*,
	0+sum(missing_comments_start) over w as seq_id
	from
	(
		select 
			sm.subreddit,
			sm.month,
			sm.created_utc + interval '1 day' * (sm.month*30) as start_period,
			sm.created_utc + interval '1 day' * ((sm.month+1)*30) as end_period,
			sm.max_month as max_month,
			creation_delta_months,

			
			
			lag(creation_delta_months) over w as prev_creation_delta_month,
			--0+sum(case when creation_delta_months is null and lag(creation_delta_months) over w = month - 1 then 1 else 0 end) over w as seq_id,
			case when creation_delta_months is null and (month = 0 or (lag(creation_delta_months) over w = month - 1)) then 1 else 0 end as missing_start,
			case when 
				creation_delta_months is not null and lag(creation_delta_months) over w is null and month  > 0 
			then 1 else 0 end as missing_end,
			total_activity,
			total_comments,
			total_submissions,
			lag(total_comments) over w as lagged_comments,

			-- coalesce(total_comments,0) as ctc,
			-- coalesce(lag(total_comments) over w,0) as lctc,
			-- lag(total_comments) over w as ltc,

			-- case when month > 0 and total_comments is null then 1 else 0 end as test1,
			-- case when (total_comments is null or total_comments = 0) then 1 else 0 end as test2,
	
			case 
				when 
					month > 0 and 
					(  coalesce(total_comments,0) = 0
						and ((lag(total_comments) over w) != 0))
				then 1 else 
					case when month = 0 and coalesce(total_comments,0) = 0 then 1 else 0 end
				end as missing_comments_start,

			case
				when month > 0
					and coalesce(total_comments,0) != 0
					and coalesce(lag(total_comments) over w, 0) = 0
				then 1 else 0 end as missing_comments_end


		
		from sub_months sm
		left join s2_subreddit_30day_activity_summary ssa on sm.subreddit = ssa.subreddit and ssa.creation_delta_months = sm.month
		window w as (partition by sm.subreddit order by sm.month asc)
		order by sm.subreddit, sm.month	
	) a
	window w as (partition by a.subreddit order by a.month asc)

)
select * from sub_month_borders





,
missing_periods as (
	select
		smb.subreddit,
		smb.seq_id,
		max(smb.max_month) as max_month,
		min(start_period) as start_period_date,
		max(end_period) as end_period_date,
		min(month) as start_month,
		max(month) as end_month,
		(max(case when missing_comments_end = 1 then month else null end) - min(case when missing_comments_start = 1 then month else null end))::int as total_missing_months,
		min(case when missing_comments_start = 1 then month else null end) as missing_start,
		max(case when missing_comments_end = 1 then month else null end) as missing_end
--		max(case when missing_start = 1 then month else null end),
--		min(case when missing_start = 1 then month else null end),
--		array_agg(creation_delta_months) as months,
--		array_agg(missing_end),
--		array_agg(missing_start)
	from sub_month_borders smb
	group by smb.subreddit, smb.seq_id
)
--select * from missing_periods



,
fixed_missing_ends as (
	select
		subreddit,
		seq_id,
		max_month,
		start_period_date,
		end_period_date,
		start_month,
		missing_start,
		coalesce(missing_end, end_month) as missing_end,
		case when missing_start is null and missing_end is null then 0 else coalesce(missing_end, end_month) - coalesce(missing_start, start_month) end as total_missing_months
--		missing_start as missing_start,
--		missing_end as missing_end
	from missing_periods mp
)
--select * from fixed_missing_ends


select
	sr.subreddit, sr.created_utc, sr.max_months, mp.missing_start, mp.missing_end, total_missing_months, start_period_date, end_period_date,
	sr.created_utc + (interval '1 day' * ((sr.max_months-1) * 30)) as last_possible_day,
	first_missing_start, sr.max_months, 
	case when first_missing_start is not null and first_missing_start < (sr.max_months-1) then 1 else 0 end as death

--into s2_subreddit_deaths
from subreddit_ranges sr
	left join
	(
		select subreddit, min(missing_start) as first_missing_start
		from fixed_missing_ends
		where total_missing_months > 1
		group by subreddit
	) a on sr.subreddit = a.subreddit
left join missing_periods mp on mp.subreddit = sr.subreddit and a.first_missing_start = mp.missing_start
--where case when first_missing_start is not null and first_missing_start < (sr.max_months-1) then 1 else 0 end = 0








--
--
--
--
--

with eligible_subreddits as (
	select
		name as subreddit,
		ss.created_utc as created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddit_summary ss
	left join s2_subreddit_30day_activity_summary sas on sas.subreddit = ss.name 
	where created_utc >= '2012-01-01' and created_utc <= '2013-01-01' 
	and total_comments > 25 and total_unique_authors >= 10
),
monthly_activity as (
	select
		es.subreddit,
		min(sas.creation_delta_months) as first_month,
		count(distinct creation_delta_months) as active_months
	from eligible_subreddits es
	left join s2_subreddit_30day_activity_summary sas on sas.subreddit = es.subreddit
	where sas.total_activity > 0
	group by es.subreddit
)


select
	es.*,
	ma.first_month,
	ma.active_months,
	sd.max_months,
	sd.missing_start,
	sd.total_missing_months,
	sd.start_period_date,
	sd.end_period_date,
	sd.death,
	smdc.*
from eligible_subreddits es
left join monthly_activity ma on ma.subreddit = es.subreddit
left join s2_subreddit_monthly_data_combined smdc on smdc.subreddit = es.subreddit
left join s2_subreddit_deaths sd on sd.subreddit = es.subreddit

where 
sd.missing_start is null or smdc.creation_delta_months < sd.missing_start
--	and sd.missing_start > 
	--and 
--	smdc.active_months >= 2
	--and 
--	smdc.lifetime_total_submissions >= 100 and smdc.lifetime_total_comments >= 25
--	 asdf
--and 
--sd.missing_start is null
--and 
--creation_delta_months = 0
--group by smdc.subreddit












--
--
--
--
--

with year_subs as (
	select 
		display_name as subreddit, 
		created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
),
threshold_data as (
	select
		ys.*, 
		usj.creation_delta_months as threshold_match_month
	from  year_subs ys
	left join s2_user_subreddit_joins usj on ys.subreddit = usj.subreddit
	where creation_delta_months is not null and creation_delta_months < (max_months-13)
	and usj.account_num = 10
)
select
	td.threshold_match_month,
	smdc.*,
	sd.missing_start - creation_delta_months -1 as remaining_lifetime,
	sd.missing_start - td.threshold_match_month - 1 as remaining_lifetime2,
	smdc.creation_delta_months - td.threshold_match_month as creation_delta_month2 
from threshold_data td
left join s2_subreddit_monthly_data_combined smdc on smdc.subreddit = td.subreddit
inner join s2_subreddit_deaths sd on sd.subreddit = smdc.subreddit
where smdc.creation_delta_months <= sd.missing_start
	and sd.missing_start > td.threshold_match_month
	and smdc.creation_delta_months >= td.threshold_match_month








--
--
--
--
--




with year_subs as (
	select 
		display_name as subreddit, 
		created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months,
		activity_threshold
	from subreddits
	cross join lateral generate_series(10,100,10) t(activity_threshold)
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
),
threshold_data as (
	select
		ys.*, 
		usj.creation_delta_months as threshold_match_month
	from  year_subs ys
	left join s2_user_subreddit_joins usj on ys.subreddit = usj.subreddit and ys.activity_threshold = usj.account_num
	where creation_delta_months is not null and creation_delta_months < (max_months-13)
)
select 
	s.*,
	td.created_utc,
	td.activity_threshold,
	td.max_months,
	td.threshold_match_month
from threshold_data td
left join s2_subreddit_monthly_data_combined s on td.subreddit = s.subreddit and td.threshold_match_month = s.creation_delta_months
order by td.subreddit, td.activity_threshold asc;









--
--
--
--
--

drop table if exists s2_m12_success_metrics;

with year_subs as (
	select 
		display_name, 
		created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
),
sub_months as (
	select 
	ys.display_name as subreddit, 
	created_utc,
	max_months,
	month as creation_delta_months

	from year_subs ys
	cross join lateral generate_series(0, floor(ys.max_months)::int) m(month)
	order by ys.display_name, month
),
monthly_joins as (
	select
		sm.subreddit,
		sm.creation_delta_months,
		count(*) as num_user_joins
	from sub_months sm
	left join s2_user_subreddit_joins usj on sm.subreddit = usj.subreddit and sm.creation_delta_months = usj.creation_delta_months
	group by sm.subreddit, sm.creation_delta_months
)
select
	sm.subreddit, 
	sm.creation_delta_months,
	s30.creation_delta_months as activity_month,
	sg30.creation_delta_months as gini_month,
	s30r.creation_delta_months as retention_month,
	mj.creation_delta_months as joins_month,

	coalesce(s30.total_activity, 0) as total_activity,
	coalesce(s30.total_submissions, 0) as total_submissions,
	coalesce(s30.total_comments, 0) as total_comments,
	coalesce(s30.unique_authors, 0) as unique_authors,
	
--	array_agg(unique_authors) over w1 as preceeding_authors,
	avg(coalesce(unique_authors,0)) over w1 as avg_authors,
	sum(unique_authors) over w1 as total_authors,

	count(s30.creation_delta_months) over w1 as num_12m_active_months,

	
--	array_agg(sg30.total_comments) over w1 as total_comments,
	avg(coalesce(sg30.total_comments,0)) over w1 as avg_comments,
	sum(sg30.total_comments) over w1 as cumsum_comments,

--	array_agg(sg30.total_submissions) over w1 as total_submissions,
	avg(coalesce(sg30.total_submissions,0)) over w1 as avg_submissions,
	sum(sg30.total_submissions) over w1 as cumsum_submissions,	

--	array_agg(round(coalesce(total_activity_gini,0), 2)) over w1 as arr_total_activity_gini,
	avg(coalesce(total_activity_gini,0)) over w1 as avg_activity_gini,
--	array_agg(round(coalesce(total_comments_gini,0), 2)) over w1 as arr_total_comments_gini,
	avg(coalesce(total_comments_gini,0)) over w1 as avg_comments_gini,
--	array_agg(round(coalesce(total_submissions_gini,0), 2)) over w1 as arr_total_submissions_gini,
	avg(coalesce(total_submissions_gini,0)) over w1 as avg_submissions_gini,

--	array_agg(coalesce(retention_rate,0)) over w1 as arr_retention_rate,
	avg(coalesce(retention_rate,0)) over w1 as avg_retention_rate,

	mj.num_user_joins as num_m12_user_joins

into s2_m12_success_metrics
from sub_months sm
left join s2_subreddit_30day_activity_summary s30 on s30.subreddit = sm.subreddit and s30.creation_delta_months = sm.creation_delta_months
left join s2_subreddit_gini_30days sg30 on sg30.subreddit = sm.subreddit and sg30.creation_delta_months = sm.creation_delta_months
left join subreddit_30day_retention_fixed s30r on sm.subreddit = s30r.subreddit and s30r.creation_delta_months = sm.creation_delta_months
left join monthly_joins mj on sm.subreddit = mj.subreddit and mj.creation_delta_months = sm.creation_delta_months

		window w as (partition by sm.subreddit order by sm.creation_delta_months asc),
		w1 as (partition by sm.subreddit order by sm.creation_delta_months range between 11 preceding and current row);


grant select on s2_m12_success_metrics to public;

create index on s2_m12_success_metrics(subreddit);
create index on s2_m12_success_metrics(subreddit, creation_delta_months);







--
--
--
--
--




with active_comment_first_months as (
  select s2.subreddit, 
    sum(case when creation_delta_months = 1 then total_activity else 0 end) as first_month_activity,
    count(distinct creation_delta_months) as active_months
    from s2_subreddit_30day_activity_summary s2
    where s2.total_comments > 0 and s2.creation_delta_months < 6  
    group by subreddit
),
threshold_data as (
	select
		subreddit, creation_delta_months
	from s2_user_subreddit_joins
	where account_num = 10 and creation_delta_months < 12
), full_data as (
select 
  ss.created_utc,
  td.creation_delta_months as activity_start_month,
  s.*
  
	from active_comment_first_months acfm
  left join subreddit_summary  ss on acfm.subreddit = ss.name
  left join s2_subreddit_monthly_data_combined s on acfm.subreddit = s.subreddit
  left join threshold_data td on td.subreddit = ss.name
	where ss.created_utc >= '2012-01-01' and ss.created_utc < '2013-01-01' and  acfm.active_months >= 3 and ss.total_submissions >= 50 and ss.total_comments >= 50 and ss.unique_authors >= 10 
	and td.creation_delta_months is not null 
)
select
	*
from full_data fd
where fd.creation_delta_months in (fd.activity_start_month+1, fd.activity_start_month+2, fd.activity_start_month+5, fd.activity_start_month+11)
	
	order by subreddit, creation_delta_months asc







--
--
--
--
--


---------------------------------------------------------------------------
--
-- s2_subreddit_monthly_data_combined
--
---------------------------------------------------------------------------


drop table if exists s2_subreddit_monthly_data_combined;


with year_subs as (
	select display_name as subreddit, created_utc, subscribers
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' and display_name is not null
),
creators as (
	select distinct subreddit, creator
	from subreddit_creator_updates
	where creator != '[deleted]'
),
mods as (
	select distinct subreddit, moderator
	from subreddit_moderator_updates
	where moderator != '[deleted]'
),
creator_and_mod_subs as (
	select
		subreddit, sum(num_creators) as num_creators, sum(num_moderators) as num_moderators
	from (
	
		select c.subreddit as subreddit, count(creator) as num_creators, NULL as num_moderators
			from creators c
			group by subreddit
		
		union
		
		select m.subreddit as subreddit, 0 as num_creators, count(moderator) as num_moderators
			from mods m
			group by subreddit
		
	) k
		
	group by subreddit
),
eligible_subs as (
	select ss.name, ys.created_utc, total_activity, total_comments, total_submissions, unique_authors, 
		case when total_submissions < 1000 then 0
		when total_submissions < 2627 then 1
		when total_submissions < 6443 then 2
		else 3
		end as submission_bin
	from year_subs ys 
	inner join subreddit_summary ss on ss.name  = ys.subreddit 
	--where total_submissions >= 400
),
first_6mo_sums as (
	select ys.subreddit,
		sum(total_activity) as total_activity,
		sum(total_comments) as total_comments,
		sum(total_submissions) as total_submissions,
		count(distinct author) as unique_authors
	from year_subs ys
	left join user_sub_activity_30day_activity usa on usa.subreddit = ys.subreddit
	where usa.creation_delta_months < 6
	group by ys.subreddit
),
seeding_content as (
	select s2.subreddit, 
	count(distinct s2.author) as num_seed_authors, 
	sum(s2.total_submissions) num_seeded_content, 
	sum(s2.is_mod) num_seeding_mods, 
	sum(case when cr.creator is not null then 1 else 0 end) as creator_seeding
	from year_subs ys
	left join s2_sub_user_retention_intermediate s2 on ys.subreddit = s2.subreddit
	left join creators cr on cr.subreddit = s2.subreddit and cr.creator = s2.author
	where creation_delta_months = 0 and s2.total_submissions >= 5 and s2.author != '[deleted]'
	group by s2.subreddit
	order by s2.subreddit asc	
),
fostering_data as (
	select
		s.subreddit, 
		s.creation_delta_months,
	
		count(distinct susd.author) as num_fostering_authors,
		sum(s.total_activity) as total_fostering_activity,
		sum(s.total_submissions) as total_fostering_submissions,
		sum(s.total_comments) as total_fostering_comments,
		sum(susd.is_mod) as num_fostering_mods,
		sum(case when susd.is_mod = 1 then s.total_activity else 0 end) as total_moderator_fostering_activity,
		sum(case when susd.is_mod = 1 then s.total_submissions else 0 end) as total_moderator_fostering_submissions,
		sum(case when susd.is_mod = 1 then s.total_comments else 0 end) as total_moderator_fostering_comments,
		sum(case when c.creator is not null then 1 else 0 end) as num_fostering_creators,
		sum(case when c.creator is not null then s.total_activity else 0 end) as total_creator_fostering_activity,
		sum(case when c.creator is not null then s.total_submissions else 0 end) as total_creator_fostering_submissions,
		sum(case when c.creator is not null then s.total_comments else 0 end) as total_creator_fostering_comments
		
	from s2_sub_user_retention_intermediate s
	left join s2_sub_user_sequence_data susd on s.subreddit = susd.subreddit and s.author = susd.author
	left join creators c on c.subreddit = s.subreddit
	where susd.total_months >= 3 and susd.total_activity > (total_months * 10) 
	and susd.first_delta_month <= s.creation_delta_months and susd.last_delta_month >= s.creation_delta_months
	
	group by s.subreddit, s.creation_delta_months
),
cumulative_data as (
	select
		s2.subreddit,
		s2.creation_delta_months,

		s2.total_activity,
		s2.total_comments,
		s2.unique_authors,

		sum(s2.total_activity) over w as s2_cum_activity,
		sum(s2.total_comments) over w as s2_cum_comments,
		sum(s2.total_submissions) over w as s2_cum_submissions,
		sum(s2.unique_authors) over w as s2_cum_unique_authors,

		avg(s2.total_activity) over w as s2_avg_activity,
		avg(s2.total_comments) over w as s2_avg_comments,
		avg(s2.total_submissions) over w as s2_avg_submissions,
		avg(s2.unique_authors) over w as s2_avg_authors,

		avg(s2.total_activity) over w1 as s2_avg_activity2,
		avg(s2.total_comments) over w1 as s2_avg_comments2,
		avg(s2.unique_authors) over w1 as s2_avg_authors2

		from eligible_subs es
	    left join s2_subreddit_30day_activity_summary s2 on s2.subreddit = es.name
		window w as (partition by s2.subreddit order by s2.creation_delta_months asc),
		w1 as (partition by s2.subreddit order by s2.creation_delta_months range unbounded preceding)

),
first_month as (
	select 
		s0.subreddit,
		s0.total_activity as first_month_total_activity,
		coalesce(s0.total_submissions, 0) as first_month_total_submissions,
		coalesce(s0.total_comments, 0) as first_month_total_comments,
		coalesce(s0.unique_authors, 0) as first_month_unique_authors,
		coalesce(sg30.total_activity_gini, 0) as first_month_activity_gini,
		coalesce(sg30.total_comments_gini, 0) as first_month_comments_gini,
		coalesce(sg30.total_submissions_gini, 0) as first_month_submissions_gini

	from eligible_subs es 
	left join s2_subreddit_30day_activity_summary s0 on s0.subreddit = es.name
	left join s2_subreddit_gini_30days sg30 on sg30.subreddit = es.name and s0.creation_delta_months = sg30.creation_delta_months
	where s0.creation_delta_months = 0 and s0.total_activity > 0
)
-- advertising_comments as (
-- 	-- we're looking for inbound links here so subreddit is the mentioned_sub
-- 	select
-- 		mentioned_sub_name as subreddit,
-- 		creation_delta_months,
-- 		count(distinct author) as num_comment_adv_authors,
-- 		count(distinct subreddit) as num_comment_adv_subreddits,
-- 		sum(total_links) as total_comment_adv_links
	
-- 	from s2_comment_sub_link_30day_counts
-- 	where mentioned_sub_name is not null
-- 	group by mentioned_sub_name, creation_delta_months
-- ),
-- advertising_submission as (
-- select
-- 	mentioned_sub_name,
-- 	creation_delta_months,
	
-- 	num_comment_adv_authors,
-- 	num_comment_adv_subreddits,
-- 	total_comment_advs_total_links,
-- 	total_comment_adv_comments,

-- 	num_submission_title_adv_authors,
-- 	num_submission_title_adv_subreddits,
-- 	total_submission_title_advs_total_links,
-- 	total_title_adv_submissions,

-- 	num_submission_selftext_adv_authors,
-- 	num_submission_selftext_adv_subreddits,
-- 	total_submission_selftext_advs_total_links,
-- 	total_selftext_adv_submissions	

-- 	from s2_subreddit_30day_inbound_advertising_data s2
-- )

	
	select
		ys.subreddit as subreddit,
		s.creation_delta_months, 

		es.total_activity as lifetime_total_activity,
		es.total_comments as lifetime_total_comments,
		es.total_submissions as lifetime_total_submissions,
		es.unique_authors as lifetime_total_unique_authors,
	
		coalesce(s.unique_authors, 0) as unique_authors, 
		coalesce(s.total_activity, 0) as total_activity,
		coalesce(s.total_comments, 0) as total_comments,
		coalesce(s.total_submissions, 0) as total_submissions,
		coalesce(s.sub_and_comment_authors, 0) as sub_and_comment_authors,
		coalesce(s.comment_only_authors, 0) as comment_only_authors,
		coalesce(s.submission_only_authors, 0) as submission_only_authors,
		coalesce(s.total_submitters, 0) as total_submitters,
		coalesce(s.total_commenters, 0) as total_commenters,
		coalesce(s.retentive_authors, 0) as retentive_authors,
		coalesce(s.retentive_activity, 0) as retentive_activity,
		coalesce(s.non_retentive_activity, 0) as non_retentive_activity,
		coalesce(s.retentive_comments, 0) as retentive_comments,
		coalesce(s.retentive_submissions, 0) as retentive_submissions,
		coalesce(s.retentive_commenters, 0) as retentive_commenters,
		coalesce(s.retentive_submitters, 0) as retentive_submitters,
		coalesce(s.has_deleted_author, 0) as has_deleted_author,
		coalesce(s.deleted_activity, 0) as deleted_activity,
		coalesce(s.deleted_submissions, 0) as deleted_submissions,
		coalesce(s.deleted_comments, 0) as deleted_comments,

		coalesce(f6s.total_comments, 0) as first_period_total_comments,
		coalesce(f6s.total_activity, 0) as first_period_total_activity,
		coalesce(f6s.total_submissions, 0) as first_period_total_submissions,
		coalesce(f6s.unique_authors, 0) as first_period_unique_authors,





		cd0.total_activity as cd0_total_activity,
		cd0.total_comments as cd0_total_comments,
		cd0.unique_authors as cd0_unique_authors,

		cd2.total_activity as cd2_total_activity,
		cd2.total_comments as cd2_total_comments,
		cd2.unique_authors as cd2_unique_authors,
	

		cd3.total_activity as cd3_total_activity,
		cd3.total_comments as cd3_total_comments,
		cd3.unique_authors as cd3_unique_authors,

		cd6.total_activity as cd6_total_activity,
		cd6.total_comments as cd6_total_comments,
		cd6.unique_authors as cd6_unique_authors,

		cd12.total_activity as cd12_total_activity,
		cd12.total_comments as cd12_total_comments,
		cd12.unique_authors as cd12_unique_authors,


		cdminus1.total_activity as prev_total_activity,
		cdminus1.total_comments as prev_total_comments,
		cdminus1.unique_authors as prev_unique_authors,


	
		cd0.s2_avg_activity as c0_avg_activity,
		cd0.s2_avg_comments as c0_avg_comments,
		cd0.s2_avg_submissions as c0_avg_submissions,
		cd0.s2_avg_authors as c0_avg_authors,
	
		cd2.s2_avg_activity as c2_avg_activity,
		cd2.s2_avg_comments as c2_avg_comments,
		cd2.s2_avg_authors as c2_avg_authors,

		cd3.s2_avg_activity as c3_avg_activity,
		cd3.s2_avg_comments as c3_avg_comments,
		cd3.s2_avg_authors as c3_avg_authors,


		cdminus1.s2_avg_activity as prev_avg_activity,
		cdminus1.s2_avg_comments as prev_avg_comments,
		cdminus1.s2_avg_submissions as prev_avg_submissions,
		cdminus1.s2_avg_authors as prev_avg_authors,


	
		--
	    -- 2nd month snapshot
	    --


		(s2.total_activity is not null) as n2_present,
		coalesce(s2.total_activity,0) as n2_total_activity,
		coalesce(s2.unique_authors,0) as n2_unique_authors,
		coalesce(s2.total_submissions,0) as n2_total_submissions,
		coalesce(s2.total_comments,0) as n2_total_comments,

		-- 2nd month snapshot: This is the fraction of activity in the predicted month to the snapshot month
		case when cd0.total_activity > 0 then (coalesce(cd2.total_activity,0)) / (cd0.total_activity::decimal) else NULL end  as n2_total_activity_rate5,
		case when cd0.total_comments > 0 then (coalesce(cd2.total_comments,0)) / (cd0.total_comments::decimal) else NULL end  as n2_total_comments_growth_rate5,
		case when cd0.unique_authors > 0 then (coalesce(cd2.unique_authors,0)) / (cd0.unique_authors::decimal) else NULL end  as n2_unique_authors_growth_rate5,
	
	

		--
		-- N3
		--

		(s3.total_activity is not null) as n3_present,
		coalesce(s3.total_activity,0) as n3_total_activity,
		coalesce(s3.unique_authors,0) as n3_unique_authors,
		coalesce(s3.total_submissions,0) as n3_total_submissions,
		coalesce(s3.total_comments,0) as n3_total_comments,	

	
		-- this is the average cumulative growth rate of this month over the previous used in Tsugawa & Niida
		--case when cd0.s2_cum_activity > 0 then ((coalesce(cd3.s2_cum_activity,0) - coalesce(cd0.s2_cum_activity,0))/3.0) / (cd0.s2_cum_activity::decimal / (s.creation_delta_months::decimal+1) ) else NULL end  as n3_total_activity_rate,
		--case when cd0.s2_cum_comments > 0 then ((coalesce(cd3.s2_cum_comments,0) - coalesce(cd0.s2_cum_comments,0))/3.0) / (cd0.s2_cum_comments::decimal / (s.creation_delta_months::decimal+1) ) else NULL end as n3_total_comments_growth_rate,
		--case when cd0.s2_cum_unique_authors > 0 then ((coalesce(cd3.s2_cum_unique_authors,0) - coalesce(cd0.s2_cum_unique_authors,0))/3.0) / (cd0.s2_cum_unique_authors::decimal / (s.creation_delta_months::decimal+1) ) else NULL end as n3_unique_authors_growth_rate,

		--case when cd0.total_activity > 0 then ((coalesce(cd3.total_activity,0) - coalesce(cd0.total_activity,0))/3.0) / (cd0.total_activity::decimal / (s.creation_delta_months::decimal+1) ) else NULL end  as n3_total_activity_rate_a,
		--case when cd0.total_comments > 0 then ((coalesce(cd3.total_comments,0) - coalesce(cd0.total_comments,0))/3.0) / (cd0.total_comments::decimal / (s.creation_delta_months::decimal+1) ) else NULL end as n3_total_comments_growth_rate_a,
		--case when cd0.unique_authors > 0 then ((coalesce(cd3.unique_authors,0) - coalesce(cd0.unique_authors,0))/3.0) / (cd0.unique_authors::decimal / (s.creation_delta_months::decimal+1) ) else NULL end as n3_unique_authors_growth_rate_a,

		-- This is the % of new growth in the specific month with cumulative amounts (so smaller percentages except in spiky growth)
		-- NOTE: I think dividing this by the predcited month rather than the prior month is probably a mistake
		--case when cd3.s2_cum_activity > 0 then ((coalesce(cd3.s2_cum_activity,0) - coalesce(cd2.s2_cum_activity,0))) / (cd3.s2_cum_activity::decimal)  else NULL end  as n3_total_activity_rate2,
		--case when cd3.s2_cum_comments > 0 then ((coalesce(cd3.s2_cum_comments,0) - coalesce(cd2.s2_cum_comments,0))) / (cd3.s2_cum_comments::decimal)  else NULL end as n3_total_comments_growth_rate2,
		--case when cd3.s2_cum_unique_authors > 0 then ((coalesce(cd3.s2_cum_unique_authors,0) - coalesce(cd2.s2_cum_unique_authors,0))) / (cd3.s2_cum_unique_authors::decimal)  else NULL end as n3_unique_authors_growth_rate2,

		-- This is the fraction of *new* activity over the previous month (also a percent without the multiplier of 100), should allow negative numbers
		--case when cd2.total_activity > 0 then (coalesce(cd3.total_activity,0) - coalesce(cd2.total_activity,0)::decimal) / (cd2.total_activity::decimal)  else NULL end  as n3_total_activity_rate3,
		--case when cd2.total_comments > 0 then (coalesce(cd3.total_comments,0) - coalesce(cd2.total_comments,0)) / (cd2.total_comments::decimal)  else NULL end as n3_total_comments_growth_rate3,
		--case when cd2.unique_authors > 0 then (coalesce(cd3.unique_authors,0) - coalesce(cd2.unique_authors,0)) / (cd2.unique_authors::decimal)  else NULL end as n3_unique_authors_growth_rate3,	

		-- simple fraction of activity over the previous month, decreases will be between 0 and 1
		--case when cd2.total_activity > 0 then (coalesce(cd3.total_activity,0)) / (cd2.total_activity::decimal) else NULL end  as n3_total_activity_rate4,
		--case when cd2.total_comments > 0 then (coalesce(cd3.total_comments,0)) / (cd2.total_comments::decimal) else NULL end  as n3_total_comments_growth_rate4,
		--case when cd2.unique_authors > 0 then (coalesce(cd3.unique_authors,0) ) / (cd2.unique_authors::decimal) else NULL end  as n3_unique_authors_growth_rate4,	

		-- This is the fraction of activity in the predicted month to the snapshot month
		case when cd0.total_activity > 0 then (coalesce(cd3.total_activity,0)) / (cd0.total_activity::decimal) else NULL end  as n3_total_activity_rate5,
		case when cd0.total_comments > 0 then (coalesce(cd3.total_comments,0)) / (cd0.total_comments::decimal) else NULL end  as n3_total_comments_growth_rate5,
		case when cd0.unique_authors > 0 then (coalesce(cd3.unique_authors,0)) / (cd0.unique_authors::decimal) else NULL end  as n3_unique_authors_growth_rate5,


		--case when cd0.total_activity > 0 then (coalesce(cd3.total_activity,0) - cd0.total_activity::decimal) / (cd0.total_activity::decimal) else NULL end  as n3_total_activity_rate6,
		--case when cd0.total_comments > 0 then (coalesce(cd3.total_comments,0) - cd0.total_comments::decimal) / (cd0.total_comments::decimal) else NULL end  as n3_total_comments_growth_rate6,
		--case when cd0.unique_authors > 0 then (coalesce(cd3.unique_authors,0) - cd0.unique_authors::decimal) / (cd0.unique_authors::decimal) else NULL end  as n3_unique_authors_growth_rate6,


		-- This is the average growth as used in Tsugawa & Niida
		--case when (cdminus1.s2_cum_activity - cdminus2.s2_cum_activity) > 0 then ((coalesce(cd0.s2_cum_activity,0) - coalesce(cdminus1.s2_cum_activity,0)) / (cdminus1.s2_cum_activity - cdminus2.s2_cum_activity)) else NULL end as prev_total_activity_growth,
		--case when (cdminus1.s2_cum_comments - cdminus2.s2_cum_comments) > 0 then ((coalesce(cd0.s2_cum_comments,0) - coalesce(cdminus1.s2_cum_comments,0)) / (cdminus1.s2_cum_comments - cdminus2.s2_cum_comments)) else NULL end as prev_total_comments_growth,
		--case when (cdminus1.s2_cum_unique_authors - cdminus2.s2_cum_unique_authors) > 0 then ((coalesce(cd0.s2_cum_unique_authors,0) - coalesce(cdminus1.s2_cum_unique_authors,0)) / (cdminus1.s2_cum_unique_authors - cdminus2.s2_cum_unique_authors)) else NULL end as prev_unique_authors_growth,

		-- fraction of new growth in the previous month (uses cumulative, so the fraction of new growth last month cumulatively)
		-- this is likely biased because the overall cumulative should keep growing, though may work well for very spiky subreddits
		--case when cdminus1.s2_cum_activity > 0 then (coalesce(cdminus1.s2_cum_activity,0) - coalesce(cdminus2.s2_cum_activity,0)) / (cdminus1.s2_cum_activity) else NULL end as prev_total_activity_growth2,
		--case when cdminus1.s2_cum_comments > 0 then (coalesce(cdminus1.s2_cum_comments,0) - coalesce(cdminus2.s2_cum_comments,0)) / (cdminus1.s2_cum_comments) else NULL end as prev_total_comments_growth2,
		--case when cdminus1.s2_cum_unique_authors > 0 then (coalesce(cdminus1.s2_cum_unique_authors,0) - coalesce(cdminus2.s2_cum_unique_authors,0)) / (cdminus1.s2_cum_unique_authors) else NULL end as prev_unique_authors_growth2,

		-- fraction of new growth in the previous month, can be negative
		--case when cdminus2.total_activity > 0 then (coalesce(cdminus1.total_activity,0) - coalesce(cdminus2.total_activity,0))::decimal / (cdminus2.total_activity) else NULL end as prev_total_activity_growth3,
		--case when cdminus2.total_comments > 0 then (coalesce(cdminus1.total_comments,0) - coalesce(cdminus2.total_comments,0))::decimal / (cdminus2.total_comments) else NULL end as prev_total_comments_growth3,
		--case when cdminus2.unique_authors > 0 then (coalesce(cdminus1.unique_authors,0) - coalesce(cdminus2.unique_authors,0))::decimal / (cdminus2.unique_authors) else NULL end as prev_unique_authors_growth3,

		-- fraction of previous month activity relative to the prior month
		--case when cdminus2.total_activity > 0 then coalesce(cdminus1.total_activity,0)::decimal / (cdminus2.total_activity) else NULL end as prev_total_activity_growth4,
		--case when cdminus2.total_comments > 0 then coalesce(cdminus1.total_comments,0)::decimal / (cdminus2.total_comments) else NULL end as prev_total_comments_growth4,
		--case when cdminus2.unique_authors > 0 then coalesce(cdminus1.unique_authors,0)::decimal / (cdminus2.unique_authors) else NULL end as prev_unique_authors_growth4,

		-- fraction of growth this month relativeto the last month (less growth would be < 1)
		case when cdminus1.total_activity > 0 then coalesce(cd0.total_activity,0)::decimal / (cdminus1.total_activity) else NULL end as total_activity_growth,
		case when cdminus1.total_comments > 0 then coalesce(cd0.total_comments,0)::decimal / (cdminus1.total_comments) else NULL end as total_comments_growth,
		case when cdminus1.unique_authors > 0 then coalesce(cd0.unique_authors,0)::decimal / (cdminus1.unique_authors) else NULL end as unique_authors_growth,	


		--case when cdminus1.total_activity > 0 then (coalesce(cd0.total_activity,0)::decimal - cdminus1.total_activity) / (cdminus1.total_activity) else NULL end as total_activity_growth2,
		--case when cdminus1.total_comments > 0 then (coalesce(cd0.total_comments,0)::decimal - cdminus1.total_comments) / (cdminus1.total_comments) else NULL end as total_comments_growth2,
		--case when cdminus1.unique_authors > 0 then (coalesce(cd0.unique_authors,0)::decimal - cdminus1.unique_authors) / (cdminus1.unique_authors) else NULL end as unique_authors_growth2,			


		(s6.total_activity is not null) as n6_present,
		coalesce(s6.total_activity,0) as n6_total_activity,
		coalesce(s6.unique_authors,0) as n6_unique_authors,
		coalesce(s6.total_submissions,0) as n6_total_submissions,
		coalesce(s6.total_comments,0) as n6_total_comments,


		--case when cd0.s2_cum_activity > 0 then ((coalesce(cd6.s2_cum_activity,0) - coalesce(cd0.s2_cum_activity,0))/6.0) / (cd0.s2_cum_activity::decimal / (s.creation_delta_months::decimal +1) ) else NULL end  as n6_total_activity_rate,
		--case when cd0.s2_cum_comments > 0 then ((coalesce(cd6.s2_cum_comments,0) - coalesce(cd0.s2_cum_comments,0))/6.0) / (cd0.s2_cum_comments::decimal / (s.creation_delta_months::decimal +1) ) else NULL end  as n6_total_comments_growth_rate,
		--case when cd0.s2_cum_unique_authors > 0 then ((coalesce(cd6.s2_cum_unique_authors,0) - coalesce(cd0.s2_cum_unique_authors,0))/6.0) / (cd0.s2_cum_unique_authors::decimal / (s.creation_delta_months::decimal +1) ) else NULL end  as n6_unique_authors_growth_rate,

		--case when cd6.s2_cum_activity > 0 then ((coalesce(cd6.s2_cum_activity,0) - coalesce(cd5.s2_cum_activity,0))) / (cd6.s2_cum_activity::decimal) else NULL end  as n6_total_activity_rate2,
		--case when cd6.s2_cum_comments > 0 then ((coalesce(cd6.s2_cum_comments,0) - coalesce(cd5.s2_cum_comments,0))) / (cd6.s2_cum_comments::decimal) else NULL end  as n6_total_comments_growth_rate2,
		--case when cd6.s2_cum_unique_authors > 0 then ((coalesce(cd6.s2_cum_unique_authors,0) - coalesce(cd5.s2_cum_unique_authors,0))) / (cd6.s2_cum_unique_authors::decimal ) else NULL end  as n6_unique_authors_growth_rate2,

		--case when cd5.total_activity > 0 then (coalesce(cd6.total_activity,0) - coalesce(cd5.total_activity,0)::decimal) / (cd5.total_activity::decimal)  else NULL end  as n6_total_activity_rate3,
		--case when cd5.total_comments > 0 then (coalesce(cd6.total_comments,0) - coalesce(cd5.total_comments,0)::decimal) / (cd5.total_comments::decimal)  else NULL end as n6_total_comments_growth_rate3,
		--case when cd5.unique_authors > 0 then (coalesce(cd6.unique_authors,0) - coalesce(cd5.unique_authors,0)::decimal) / (cd5.unique_authors::decimal)  else NULL end as n6_unique_authors_growth_rate3,	

		--case when cd5.total_activity > 0 then (coalesce(cd6.total_activity,0)::decimal) / (cd5.total_activity::decimal) else NULL end  as n6_total_activity_rate4,
		--case when cd5.total_comments > 0 then (coalesce(cd6.total_comments,0)::decimal) / (cd5.total_comments::decimal) else NULL end  as n6_total_comments_growth_rate4,
		--case when cd5.unique_authors > 0 then (coalesce(cd6.unique_authors,0)::decimal ) / (cd5.unique_authors::decimal) else NULL end  as n6_unique_authors_growth_rate4,


		case when cd0.total_activity > 0 then (coalesce(cd6.total_activity,0)::decimal) / (cd0.total_activity::decimal) else NULL end  as n6_total_activity_rate5,
		case when cd0.total_comments > 0 then (coalesce(cd6.total_comments,0)::decimal) / (cd0.total_comments::decimal) else NULL end  as n6_total_comments_growth_rate5,
		case when cd0.unique_authors > 0 then (coalesce(cd6.unique_authors,0)::decimal ) / (cd0.unique_authors::decimal) else NULL end  as n6_unique_authors_growth_rate5,
	

		--case when cd0.total_activity > 0 then (coalesce(cd6.total_activity,0)::decimal - cd0.total_activity::decimal) / (cd0.total_activity::decimal) else NULL end  as n6_total_activity_rate6,
		--case when cd0.total_comments > 0 then (coalesce(cd6.total_comments,0)::decimal - cd0.total_comments::decimal) / (cd0.total_comments::decimal) else NULL end  as n6_total_comments_growth_rate6,
		--case when cd0.unique_authors > 0 then (coalesce(cd6.unique_authors,0)::decimal - cd0.unique_authors::decimal) / (cd0.unique_authors::decimal) else NULL end  as n6_unique_authors_growth_rate6,

		--case when (cd5.s2_cum_activity-cd4.s2_cum_activity) > 0 then ((cd6.s2_cum_activity - cd5.s2_cum_activity) / (cd5.s2_cum_activity - cd4.s2_cum_activity)) else 0 end  as n6_prev_total_activity_growth,
		--case when (cd5.s2_cum_comments-cd4.s2_cum_comments) > 0 then ((cd6.s2_cum_comments - cd5.s2_cum_comments) / (cd5.s2_cum_comments - cd4.s2_cum_comments)) else 0 end  as n6_prev_total_comments_growth,
		--case when (cd5.unique_authors-cd4.unique_authors) > 0 then ((cd6.s2_cum_unique_authors - cd5.s2_cum_unique_authors) / (cd5.s2_cum_unique_authors - cd4.s2_cum_unique_authors)) else 0 end as n6_prev_unique_authors_growth,



		coalesce(fd.num_fostering_authors, 0) as num_fostering_authors,
		coalesce(fd.total_fostering_activity, 0) as total_fostering_activity,
		coalesce(fd.total_fostering_submissions, 0) as total_fostering_submissions,
		coalesce(fd.total_fostering_comments, 0) as total_fostering_comments,
		coalesce(fd.num_fostering_mods, 0) as num_fostering_mods,

		avg(coalesce(fd.num_fostering_authors, 0)) over w as avg_fostering_authors,
		coalesce(sum(fd.total_fostering_activity) over w,0) as fostering_cumsum_total_activity,
		coalesce(sum(fd.total_fostering_submissions) over w,0) as fostering_cumsum_total_submissions,

		sum(coalesce(fd.total_fostering_submissions,0)) over w as cumsum_fostering_submissions,
		avg(coalesce(fd.total_fostering_submissions,0)) over w as avg_fostering_submissions,
		sum(coalesce(fd.total_fostering_comments,0)) over w as cumsum_fostering_comments,
		avg(coalesce(fd.total_fostering_comments,0)) over w as avg_fostering_comments,

	
		coalesce(fd.total_moderator_fostering_activity, 0) as total_moderator_fostering_activity,
		coalesce(fd.total_moderator_fostering_submissions, 0) as total_moderator_fostering_submissions,
		coalesce(fd.total_moderator_fostering_comments, 0) as total_moderator_fostering_comments,
		coalesce(fd.num_fostering_creators, 0) as num_fostering_creators,
		coalesce(fd.total_creator_fostering_activity, 0) as total_creator_fostering_activity,
		coalesce(fd.total_creator_fostering_submissions, 0) as total_creator_fostering_submissions,
		coalesce(fd.total_creator_fostering_comments, 0) as total_creator_fostering_comments,

		case when cam.num_moderators is not null and cam.num_moderators > 0 then 1 else 0 end as sub_has_mod_data,
		case when cam.num_creators is not null and cam.num_creators > 0 then 1 else 0 end as sub_has_creator_data,
	
		coalesce(sd.num_seed_authors, 0) as num_seed_authors, 
		coalesce(sd.num_seeded_content, 0) as num_seeded_content, 
		coalesce(sd.num_seeding_mods, 0) as num_seeding_mods, 
		coalesce(sd.creator_seeding, 0) as creator_seeding,

		--coalesce(ac.num_comment_adv_authors,0) as num_comment_adv_authors,
		--coalesce(ac.num_comment_adv_subreddits,0) as num_comment_adv_subreddits,
		--coalesce(ac.total_comment_adv_links, 0) as total_comment_adv_links,

		coalesce(iad.num_comment_adv_authors, 0) as num_comment_adv_authors,
		coalesce(iad.num_comment_adv_subreddits, 0) as num_comment_adv_subreddits,
		coalesce(iad.total_comment_adv_links_or_mentions, 0) as total_comment_adv_links_or_mentions,
		coalesce(iad.total_comment_adv_comments, 0) as total_comment_adv_comments,

		coalesce(iad.comment_adv_link_comments, 0) as total_comment_adv_link_comments,
		coalesce(iad.comment_adv_mention_comments, 0) as total_comment_adv_mention_comments,

		coalesce(iad.num_submission_adv_subreddits, 0) as num_submission_adv_subreddits,
		coalesce(iad.num_submission_adv_authors, 0) as num_submission_adv_authors,
		coalesce(total_submission_adv_submissions, 0) as total_submission_adv_submissions,
		coalesce(num_submission_adv_non_deleted_authors, 0) as num_submission_adv_non_deleted_authors,
	
		coalesce(iad.submission_adv_distinct_title_authors, 0) as num_submission_title_adv_authors,
		coalesce(iad.submission_adv_distinct_title_subreddits, 0) as num_submission_title_adv_subreddits,
		coalesce(iad.total_submission_adv_title_links_and_mentions, 0) as total_submission_title_advs_total_links,
		coalesce(iad.submission_adv_distinct_title_submissions, 0) as total_title_adv_submissions,
		
		coalesce(iad.submission_adv_distinct_selftext_authors, 0) as num_submission_selftext_adv_authors,
		coalesce(iad.submission_adv_distinct_selftext_subreddits, 0) as num_submission_selftext_adv_subreddits,
		coalesce(iad.total_submission_adv_selftext_links_and_mentions, 0) as total_submission_selftext_advs_total_links,
		coalesce(iad.submission_adv_distinct_selftext_submissions, 0) as total_selftext_adv_submissions,

		coalesce(iad.num_crosspost_authors, 0) as num_crosspost_authors,
		coalesce(iad.num_crosspost_nondeleted_authors, 0) as num_crosspost_nondeleted_authors,
		coalesce(iad.num_crosspost_subreddits, 0) as num_crosspost_subreddits,
		coalesce(iad.num_crosspost_submissions, 0) as num_crosspost_submissions,



		sum(coalesce(iad.total_submission_adv_submissions,0)) over w as cumsum_adv_submissions,
		avg(coalesce(iad.total_submission_adv_submissions,0)) over w as avg_adv_submissions,
		sum(coalesce(iad.total_comment_adv_comments,0)) over w as cumsum_adv_comments,
		avg(coalesce(iad.total_comment_adv_comments,0)) over w as avg_adv_comments,
		sum(coalesce(iad.num_crosspost_submissions,0)) over w as cumsum_adv_crosspost_submissions,
		avg(coalesce(iad.num_crosspost_submissions,0)) over w as avg_adv_crosspost_submissions,


		case when s.creation_delta_months = 0 then 0 else coalesce(fm.first_month_total_activity, 0) end as first_month_total_activity,
		case when s.creation_delta_months = 0 then 0 else coalesce(fm.first_month_total_submissions, 0) end as first_month_total_submissions,
		case when s.creation_delta_months = 0 then 0 else coalesce(fm.first_month_total_comments, 0) end as first_month_total_comments,
		case when s.creation_delta_months = 0 then 0 else coalesce(fm.first_month_unique_authors,0) end as first_month_unique_authors,
		coalesce(fm.first_month_activity_gini, 0) as first_month_activity_gini,
		coalesce(fm.first_month_comments_gini, 0) as first_month_comments_gini,
		coalesce(fm.first_month_submissions_gini, 0) as first_month_submissions_gini,	

		coalesce(cd0.s2_cum_activity, 0) as cumsum_total_activity,
		coalesce(cd0.s2_cum_submissions, 0) as cumsum_total_submissions,
		coalesce(cd0.s2_cum_comments, 0) as cumsum_total_comments,


		coalesce(sg30.total_activity_gini,0) as total_activity_gini,
		coalesce(sg30.total_comments_gini,0) as total_comments_gini,
		coalesce(sg30.total_submissions_gini,0) as total_submissions_gini,

		l3a.last_3_months as active_users_2years,
		l3a.total_unique_authors as total_unique_authors_2years,
		l3a.pct_active_last_3mo as pct_active_last_3mo_2years,

		s30r.retention_rate,
		s30r.turnover_rate,

		coalesce(ssem.num_pos_engaged, 0) as num_submissions_pos_engaged,
		coalesce(ssem.num_pos_engaged_no_automod, 0) as num_submissions_pos_engaged_no_automod,
		coalesce(ssem.num_any_engaged, 0) as num_submissions_any_engaged,
		coalesce(ssem.has_comments, 0) as num_submissions_with_comments,
		case when coalesce(ssem.total_submissions,0) = 0 then 0 else coalesce(ssem.has_comments, 0)::decimal / ssem.total_submissions::decimal end as pct_submissions_with_comments,
		case when coalesce(ssem.total_submissions, 0) = 0 then 0 else coalesce(ssem.num_pos_engaged, 0)::decimal / ssem.total_submissions::decimal end as pct_submissions_with_engagement,
		case when coalesce(ssem.total_submissions, 0) = 0 then 0 else coalesce(ssem.num_pos_engaged_no_automod::decimal, 0) / ssem.total_submissions::decimal end as pct_submissions_with_engagement_no_automod,


	m12suc.total_activity as n12_total_activity,
	m12suc.total_submissions as n12_total_submissions,
	m12suc.total_comments as n12_total_comments,
	m12suc.unique_authors as n12_unique_authors,


	m12suc.avg_authors as n12_avg_authors,

	m12suc.num_12m_active_months as n12_num_active_months,
	m12suc.avg_comments as n12_avg_comments,
	m12suc.cumsum_comments as n12_cumsum_comments,

	m12suc.avg_submissions as n12_avg_submissions,
	m12suc.cumsum_submissions as n12_cumsum_submissions,	

	m12suc.avg_activity_gini as n12_avg_activity_gini,
	m12suc.avg_comments_gini as n12_avg_comments_gini,

	m12suc.avg_submissions_gini as n12_avg_submissions_gini,


	m12suc.avg_retention_rate as n12_avg_retention_rate,

	m12suc.num_m12_user_joins as n12_count_new_users
		
		

	into s2_subreddit_monthly_data_combined
	
	from year_subs ys 
	left join first_month fm on fm.subreddit = ys.subreddit
	left join eligible_subs es on es.name = ys.subreddit
	left join s2_subreddit_30day_activity_summary s on s.subreddit = ys.subreddit
	left join s2_subreddit_30day_activity_summary s2 on s2.subreddit = ys.subreddit and s2.creation_delta_months = s.creation_delta_months+2
	left join s2_subreddit_30day_activity_summary s3 on s3.subreddit = ys.subreddit and s3.creation_delta_months = s.creation_delta_months+3
	left join s2_subreddit_30day_activity_summary s6 on s6.subreddit = ys.subreddit and s6.creation_delta_months = s.creation_delta_months+6
	left join cumulative_data cd0 on cd0.subreddit = ys.subreddit and cd0.creation_delta_months = s.creation_delta_months
	left join cumulative_data cdminus1 on cdminus1.subreddit = ys.subreddit and cdminus1.creation_delta_months = s.creation_delta_months-1
	left join cumulative_data cdminus2 on cdminus2.subreddit = ys.subreddit and cdminus2.creation_delta_months = s.creation_delta_months-2
	--left join cumulative_data cd1 on cd1.subreddit = fm.subreddit and cd1.creation_delta_months = s.creation_delta_months+1
	left join cumulative_data cd2 on cd2.subreddit = ys.subreddit and cd2.creation_delta_months = s.creation_delta_months+2
	left join cumulative_data cd3 on cd3.subreddit = ys.subreddit and cd3.creation_delta_months = s.creation_delta_months+3
	--left join cumulative_data cd4 on cd4.subreddit = fm.subreddit and cd4.creation_delta_months = s.creation_delta_months+4
	left join cumulative_data cd5 on cd5.subreddit = ys.subreddit and cd5.creation_delta_months = s.creation_delta_months+5
	left join cumulative_data cd6 on cd6.subreddit = ys.subreddit and cd6.creation_delta_months = s.creation_delta_months+6
	left join cumulative_data cd12 on cd12.subreddit = ys.subreddit and cd12.creation_delta_months = s.creation_delta_months+12	
	left join fostering_data fd on fd.subreddit = ys.subreddit and fd.creation_delta_months = s.creation_delta_months
	left join seeding_content sd on sd.subreddit = ys.subreddit
	left join creator_and_mod_subs cam on cam.subreddit = ys.subreddit
	--left join advertising_comments ac on ac.subreddit = fm.subreddit and ac.creation_delta_months = s.creation_delta_months
	left join s2_subreddit_30day_inbound_advertising_data iad on ys.subreddit = iad.mentioned_sub_name and s.creation_delta_months = iad.creation_delta_months
	left join s2_subreddit_gini_30days sg30 on sg30.subreddit = ys.subreddit and sg30.creation_delta_months = s.creation_delta_months
	left join first_6mo_sums f6s on ys.subreddit = f6s.subreddit
	left join s2_last_3month_activity_2years l3a on l3a.subreddit = ys.subreddit
	left join subreddit_30day_retention_fixed s30r on s30r.subreddit = ys.subreddit and s30r.creation_delta_months = s.creation_delta_months
	left join s2_submissions_engagement_monthly ssem on ssem.subreddit = ys.subreddit and ssem.creation_delta_months = s.creation_delta_months
	left join s2_m12_success_metrics m12suc on m12suc.subreddit = ys.subreddit and m12suc.creation_delta_months = s.creation_delta_months+12
	
	window w as (partition by fm.subreddit order by s.creation_delta_months asc);
	;


grant select on s2_subreddit_monthly_data_combined to public;


create index on s2_subreddit_monthly_data_combined(subreddit);
create index on s2_subreddit_monthly_data_combined(creation_delta_months);







--
--
--
--
--


with year_subs as (
	select display_name as subreddit, created_utc, subscribers
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' and display_name is not null
),
first_activities as (
select
	ys.subreddit,
	ys.created_utc,
	ss.first_activity_time,
	(extract(epoch from (ss.first_activity_time - ys.created_utc))::bigint) / (3600*24)::int as days_since_creation,
	ss.total_submissions,
	ss.total_comments,
	ss.unique_authors
from year_subs ys
left join subreddit_summary ss on ss.name = ys.subreddit
where unique_authors >= 10 and total_submissions >= 100 and total_comments >= 100 
)
select fa.*, 

	smdc.n2_unique_authors_growth_rate5,
	smdc.n3_unique_authors_growth_rate5,
	smdc.n6_unique_authors_growth_rate5,

	smdc.lifetime_total_activity,
	smdc.lifetime_total_comments,
	smdc.lifetime_total_submissions,
	smdc.lifetime_total_unique_authors,

	smdc.first_month_total_activity,
	smdc.first_month_total_submissions,
	smdc.first_month_total_comments,
	smdc.first_month_unique_authors,

	smdc.first_month_activity_gini,
	smdc.first_month_submissions_gini,
	smdc.first_month_comments_gini,

	smdc.num_fostering_authors,
	smdc.total_fostering_submissions,
	smdc.total_fostering_comments,
	
	smdc.num_seed_authors,
	smdc.num_seeded_content,
	
	smdc.num_comment_adv_authors,
	smdc.total_comment_adv_comments,
	smdc.num_submission_adv_authors,
	smdc.total_submission_adv_submissions,

	smdc.total_activity_gini,
	smdc.total_comments_gini,
	smdc.total_submissions_gini
	
from first_activities fa
left join s2_subreddit_monthly_data_combined smdc on smdc.subreddit = fa.subreddit
where days_since_creation < 30 and smdc.creation_delta_months in (1,2,5)







--
--
--
--
--









--
--
--
--
--










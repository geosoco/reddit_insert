--
--
--
--
--

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
year_subs as (
	select 
		vs.subreddit, 
		created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from valid_subs vs
	left join subreddits s on s.display_name = vs.subreddit
	--cross join lateral generate_series(10,100,10) t(activity_threshold)
),
threshold_data as (
	select
		ys.*, 
		usj.creation_delta_months as threshold_match_month
	from  year_subs ys
	--left join s2_user_subreddit_joins usj on ys.subreddit = usj.subreddit and ys.activity_threshold = usj.account_num
	left join s2_user_subreddit_joins usj on ys.subreddit = usj.subreddit
	where creation_delta_months is not null and creation_delta_months < (max_months-13)
	and usj.account_num = 10
),
user_activity as (
	select
		vs.subreddit,

		sum(case when account_type = 'creator' then num_accounts else 0 end) as num_creator_accounts,
		sum(case when account_type = 'creator' then total_activity else 0 end) as creator_activity,

		sum(case when account_type = 'mod' then num_accounts else 0 end) as num_mod_accounts,
		sum(case when account_type = 'mod' then total_activity else 0 end) as mod_activity,

		sum(case when account_type = 'normal' then num_accounts else 0 end) as num_normal_accounts,
		sum(case when account_type = 'normal' then total_activity else 0 end) as normal_activity
		
		
	from valid_subs vs
	left join s2_subreddit_account_type_activity atm on atm.subreddit = vs.subreddit
	group by vs.subreddit
)
select 
	s.subreddit,
	s.unique_authors,
	s.lifetime_total_unique_authors,
	s.lifetime_total_activity,
	ys.created_utc,	
--	td.created_utc,
--	td.activity_threshold,
--	td.max_months,
--	td.threshold_match_month,
	ua.num_creator_accounts,
	ua.creator_activity,
	ua.num_mod_accounts,
	ua.mod_activity,
	ua.num_normal_accounts,
	ua.normal_activity

from valid_subs vs
left join year_subs ys on ys.subreddit = vs.subreddit
left join s2_subreddit_monthly_data_combined s on ys.subreddit = s.subreddit --and ys.threshold_match_month = s.creation_delta_months
left join user_activity ua on ua.subreddit = ys.subreddit


--from threshold_data td
--left join s2_subreddit_monthly_data_combined s on td.subreddit = s.subreddit and td.threshold_match_month = s.creation_delta_months
--left join user_activity ua on ua.subreddit = td.subreddit



--
--
--
--
--

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
year_subs as (
	select 
		vs.subreddit, 
		created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from valid_subs vs
	left join subreddits s on s.display_name = vs.subreddit
	--cross join lateral generate_series(10,100,10) t(activity_threshold)
),
threshold_data as (
	select
		ys.*, 
		usj.creation_delta_months as threshold_match_month
	from  year_subs ys
	--left join s2_user_subreddit_joins usj on ys.subreddit = usj.subreddit and ys.activity_threshold = usj.account_num
	left join s2_user_subreddit_joins usj on ys.subreddit = usj.subreddit
	where creation_delta_months is not null and creation_delta_months < (max_months-13)
	and usj.account_num = 10
),
user_activity as (
	select
		vs.subreddit,

		sum(case when account_type = 'creator' then num_accounts else 0 end) as num_creator_accounts,
		sum(case when account_type = 'creator' then total_activity else 0 end) as creator_activity,

		sum(case when account_type = 'mod' then num_accounts else 0 end) as num_mod_accounts,
		sum(case when account_type = 'mod' then total_activity else 0 end) as mod_activity,

		sum(case when account_type = 'normal' then num_accounts else 0 end) as num_normal_accounts,
		sum(case when account_type = 'normal' then total_activity else 0 end) as normal_activity
		
		
	from valid_subs vs
	left join s2_subreddit_account_type_activity atm on atm.subreddit = vs.subreddit
	group by vs.subreddit
)
select
	count(distinct subreddit)
	from user_activity



--
--
--
--
--

select
	death,
	account_type,
	count(distinct subreddit) as num_subreddits,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments

from s2_creator_mods_30day_activity_all_users atma

group by death, account_type;




select * from s2_creator_mods_30day_activity_all_users atma limit 1000;




--
--
--
--
--

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
user_activity as (
	select
		count(*) filter (where total_activity = 1) as num_single_item_authors,
		sum(total_activity) as total_activity,
		sum(total_activity) filter (where total_activity = 1) as single_item_author_activity
	from eligible_subs es
	left join user_subreddit_activity usa on usa.subreddit = es.subreddit
)
select * from user_activity

--
--
--
--
--




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
creators as (
	select distinct vs.subreddit, creator
	from valid_subs vs
	left join subreddit_creator_updates scu on scu.subreddit = vs.subreddit
	where creator != '[deleted]' 
),
moderators as (
	select distinct vs.subreddit, moderator
	from valid_subs vs
	left join subreddit_moderator_updates smu on smu.subreddit = vs.subreddit
	where moderator != '[deleted]'
),
mods_and_creator_subs as (
	select subreddit, sum(a.num_mods), sum(a.num_creators), bool_or(has_mods) as has_moderator, bool_or(has_creator) as has_creator
	from (
		select vs.subreddit, count(distinct case when moderator != '[deleted]' then moderator else NULL end) as num_mods, 0 as num_creators,
			bool_or(moderator is not NULL) as has_mods, TRUE as has_creator
		from valid_subs vs
		left join subreddit_moderator_updates smu on smu.subreddit = vs.subreddit

		group by vs.subreddit
	
		union all
	
		select vs.subreddit, 0 as num_mods, count(distinct case when creator != '[deleted]' then creator else null end) as num_creators, 
		FALSE as has_mods, bool_or(creator is not null) as has_creator
		from valid_subs vs
		left join subreddit_creator_updates scu on scu.subreddit = vs.subreddit
		group by vs.subreddit
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
	left join s2_sub_user_sequence_data2 susd on macs.subreddit = susd.subreddit
	where susd.author != '[deleted]' and susd.total_months >= 3
	
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
mods_and_creators as (
	select
		vs.subreddit, account, is_mod, is_creator, is_mod_or_creator
	from valid_subs vs 
	left join s2_mods_and_creators mac on mac.subreddit = vs.subreddit
	where account != '[deleted]'
),
sub_foster_data as (
	select
		suri.creation_delta_months,
		case when macs.is_creator = 1 then 'creator'
			when macs.is_mod = 1 then 'mod'
			else 'normal' end as account_type,
		'fostering' as activity_type,
		count(distinct susd.author) as num_accounts,
		count(distinct susd.subreddit) as num_subreddits,
		sum(suri.total_activity) as total_activity,
		sum(suri.total_submissions) as total_submissions,
		sum(suri.total_comments) as total_comments

	from valid_subs ms
	left join s2_sub_user_sequence_data2 susd on ms.subreddit = susd.subreddit
	left join mods_and_creators macs on ms.subreddit = macs.subreddit and macs.account = susd.author
	left join s2_sub_user_retention_intermediate suri on suri.subreddit = susd.subreddit and suri.author = susd.author and suri.creation_delta_months >= susd.first_delta_month and suri.creation_delta_months <= susd.last_delta_month
	where susd.author != '[deleted]'  and susd.total_months >= 3
	group by suri.creation_delta_months, account_type
	order by suri.creation_delta_months, account_type
)
select * from sub_foster_data
union all
select 
	b.creation_delta_months,
	b.account_type,
	'all' as activity_type,
	b.num_accounts,
	b.num_subreddits,
	b.total_activity,
	b.total_submissions,
	b.total_comments
from s2_account_type_monthly_activity b


--
--
--
--
--

with dead_subs as (
	select  
		subreddit, coalesce(missing_end, max_months) as missing_end
	from s2_subreddit_deaths
	where death = 1
),
post_death_activity as (
select
	ds.subreddit, ds.missing_end, 
	count(*) as total_active_months,
	count(*) filter (where unique_authors >= 5) as total_active_months_5users,
	count(*) filter (where unique_authors >= 10) as total_active_months_10users,
	count(*) filter (where unique_authors >= 100) as total_active_months_100users
from dead_subs ds
left join s2_subreddit_30day_activity_summary sas on sas.subreddit = ds.subreddit and sas.creation_delta_months > missing_end
group by ds.subreddit, ds.missing_end
)
select * from post_death_activity
where total_active_months_10users > 4


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
			bool_or(moderator is not NULL) as has_mods, FALSE as has_creator
		from moderators  group by subreddit
	
		union all
	
		select subreddit, 0 as num_mods, count(distinct case when creator != '[deleted]' then creator else null end) as num_creators, 
		FALSE as has_mods, bool_or(creator is not null) as has_creator
		from creators group by subreddit
	) a
	group by subreddit
),
active_subs as (
	select
		macs.subreddit, smdc.creation_delta_months, smdc.total_activity
	from mods_and_creator_subs macs
	left join s2_subreddit_monthly_data_combined smdc on smdc.subreddit = macs.subreddit
),
combined_data as (
	select 
		aasa.*,
		case when m.moderator is not null then 1 else 0 end as is_mod,
		case when c.creator is not null then 1 else 0 end as is_creator,
		case when m.moderator is not null or c.creator is not null then 1 else 0 end as is_mod_or_creator,
		case when act.creation_delta_months is not null and act.total_activity > 0 then 1 else 0 end as sub_active,
		mac.has_creator,
		sd.death
	from s2_adv_author_sub_30day_activity aasa
	left join mods_and_creator_subs mac on lower(mac.subreddit) = lower(aasa.mentioned_sub_name)
	left join moderators m on m.subreddit = mac.subreddit and m.moderator = aasa.author
	left join creators c on c.subreddit = mac.subreddit and c.creator = aasa.author	
	left join active_subs act on act.subreddit = mac.subreddit and act.creation_delta_months = aasa.creation_delta_months
	left join s2_subreddit_deaths sd on mac.subreddit = sd.subreddit
	where aasa.author != '[deleted]'
)
select
	*
from combined_data
where sub_active = 0

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
		aasa.*,
		case when m.moderator is not null then 1 else 0 end as is_mod,
		case when c.creator is not null then 1 else 0 end as is_creator,
		case when m.moderator is not null or c.creator is not null then 1 else 0 end as is_mod_or_creator
	from mods_and_creator_subs macs
	left join s2_adv_author_sub_activity aasa on macs.subreddit = aasa.mentioned_sub_name
	left join moderators m on m.subreddit = aasa.mentioned_sub_name and m.moderator = aasa.author
	left join creators c on c.subreddit = aasa.mentioned_sub_name and c.creator = aasa.author

)
select
	*
from combined_data

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
	from s2_adv_author_sub_30day_activity aasa
	left join mods_and_creators mac on lower(mac.subreddit) = lower(aasa.mentioned_sub_name) and mac.account = aasa.author
	where aasa.author != '[deleted]'
)
select
	*
from combined_data



--
--
--
--
--

with mods_and_creators as (
	select
		distinct subreddit
	from s2_mods_and_creators
)
select
	mac.subreddit,
	siad.*
from mods_and_creators mac
left join s2_subreddit_30day_inbound_advertising_data siad on siad.mentioned_sub_name = mac.subreddit
where siad.mentioned_sub_name is not null

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




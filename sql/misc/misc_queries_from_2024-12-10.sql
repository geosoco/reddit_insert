--
--
-- misc queries from 12/10/2024
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
	and usj.account_num = 100
),
user_activity as (
	select
		td.subreddit,

		sum(case when account_type = 'creator' then num_accounts else 0 end) as num_creator_accounts,
		sum(case when account_type = 'creator' then total_activity else 0 end) as creator_activity,

		sum(case when account_type = 'mod' then num_accounts else 0 end) as num_mod_accounts,
		sum(case when account_type = 'mod' then total_activity else 0 end) as mod_activity,

		sum(case when account_type = 'normal' then num_accounts else 0 end) as num_normal_accounts,
		sum(case when account_type = 'normal' then total_activity else 0 end) as normal_activity
		
		
	from threshold_data td
	left join s2_subreddit_account_type_activity atm on atm.subreddit = td.subreddit
	group by td.subreddit
),
mods_and_creators as (
	select
		td.subreddit, account, is_mod, is_creator, is_mod_or_creator
	from threshold_data td
	left join s2_mods_and_creators mac on mac.subreddit = td.subreddit
	where account != '[deleted]'
),
sub_foster_data as (
	select
		td.subreddit,
		suri.creation_delta_months,
		case when macs.is_creator = 1 then 'creator'
			when macs.is_mod = 1 then 'mod'
			else 'normal' end as account_type,
		count(distinct susd.author) as num_accounts,
		count(distinct susd.subreddit) as num_subreddits,
		sum(suri.total_activity) as total_activity,
		sum(suri.total_submissions) as total_submissions,
		sum(suri.total_comments) as total_comments

	from threshold_data td
	left join s2_sub_user_sequence_data2 susd on td.subreddit = susd.subreddit
	left join s2_sub_user_retention_intermediate suri on suri.subreddit = susd.subreddit and suri.author = susd.author and suri.creation_delta_months >= susd.first_delta_month and suri.creation_delta_months <= susd.last_delta_month
	left join mods_and_creators macs on td.subreddit = macs.subreddit and macs.account = susd.author
	where susd.author != '[deleted]'  and susd.total_months >= 3 and suri.creation_delta_months = 0
	group by td.subreddit, suri.creation_delta_months, account_type
	--order by subreddit, suri.creation_delta_months, account_type
),
first_month_sfd as (
	select
		subreddit,

		sum(num_accounts) filter (where account_type = 'creator') as num_fostering_creator_accounts,
		sum(total_activity) filter (where account_type = 'creator') as fostering_creator_activity,
		sum(total_submissions) filter (where account_type = 'creator') as fostering_creator_submissions,
		sum(total_comments) filter (where account_type = 'creator') as fostering_creator_comments,

		sum(num_accounts) filter (where account_type = 'mod') as num_fostering_mod_accounts,
		sum(total_activity) filter (where account_type = 'mod') as fostering_mod_activity,
		sum(total_submissions) filter (where account_type = 'mod') as fostering_mod_submissions,
		sum(total_comments) filter (where account_type = 'mod') as fostering_mod_comments,

		sum(num_accounts) filter (where account_type = 'normal') as num_fostering_normal_accounts,
		sum(total_activity) filter (where account_type = 'normal') as fostering_normal_activity,
		sum(total_submissions) filter (where account_type = 'normal') as fostering_normal_submissions,
		sum(total_comments) filter (where account_type = 'normal') as fostering_normal_comments,

		sum(total_activity) as total_fostering_activity,
		sum(total_submissions) as total_fostering_submissions,
		sum(total_comments) as total_fostering_comments

	from sub_foster_data sfd
	group by subreddit
)
select 
	s.subreddit,
	s.unique_authors,
	s.lifetime_total_unique_authors,
	s.lifetime_total_activity,
	td.created_utc,
--	td.activity_threshold,
	td.max_months,
	td.threshold_match_month,
	ua.num_creator_accounts,
	ua.creator_activity,
	ua.num_mod_accounts,
	ua.mod_activity,
	ua.num_normal_accounts,
	ua.normal_activity,
	
	coalesce(fms.num_fostering_creator_accounts, 0) as num_fostering_creator_accounts,
	coalesce(fms.fostering_creator_activity, 0) as fostering_creator_activity,
	coalesce(fms.fostering_creator_submissions, 0) as fostering_creator_submissions,
	coalesce(fms.fostering_creator_comments, 0) as fostering_creator_comments,
	
	coalesce(fms.num_fostering_mod_accounts, 0) as num_fostering_mod_accounts,
	coalesce(fms.fostering_mod_activity, 0) as fostering_mod_activity,
	coalesce(fms.fostering_mod_submissions, 0) as fostering_mod_submissions,
	coalesce(fms.fostering_mod_comments, 0) as fostering_mod_comments,
	
	coalesce(fms.num_fostering_normal_accounts, 0) as num_fostering_normal_accounts,
	coalesce(fms.fostering_normal_activity, 0) as fostering_normal_activity,
	coalesce(fms.fostering_normal_submissions, 0) as fostering_normal_submissions,
	coalesce(fms.fostering_normal_comments, 0) as fostering_normal_comments,

	coalesce(fms.total_fostering_activity,0) as total_fostering_activity,
	coalesce(fms.total_fostering_submissions,0) as total_fostering_submissions,
	coalesce(fms.total_fostering_comments,0) as total_fostering_comments

	-- These are at the threshold month which isn't as useful
	--s.total_fostering_activity,
	--s.total_fostering_submissions,
	--s.total_fostering_comments

from threshold_data td
left join s2_subreddit_monthly_data_combined s on td.subreddit = s.subreddit and td.threshold_match_month = s.creation_delta_months
left join user_activity ua on ua.subreddit = td.subreddit
left join first_month_sfd fms on td.subreddit = fms.subreddit









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
mods_and_creators as (
	select
		ys.subreddit, account, is_mod, is_creator, is_mod_or_creator
	from year_subs ys
	left join s2_mods_and_creators mac on mac.subreddit = ys.subreddit
	where account != '[deleted]'
),
sub_foster_data as (
	select
		ys.subreddit,
		suri.creation_delta_months,
		case when macs.is_creator = 1 then 'creator'
			when macs.is_mod = 1 then 'mod'
			else 'normal' end as account_type,
		count(distinct susd.author) as num_accounts,
		count(distinct susd.subreddit) as num_subreddits,
		sum(suri.total_activity) as total_activity,
		sum(suri.total_submissions) as total_submissions,
		sum(suri.total_comments) as total_comments

	from year_subs ys
	left join s2_sub_user_sequence_data2 susd on ys.subreddit = susd.subreddit
	left join s2_sub_user_retention_intermediate suri on suri.subreddit = susd.subreddit and suri.author = susd.author and suri.creation_delta_months >= susd.first_delta_month and suri.creation_delta_months <= susd.last_delta_month
	left join mods_and_creators macs on ys.subreddit = macs.subreddit and macs.account = susd.author
	where susd.author != '[deleted]'  and susd.total_months >= 3 and suri.creation_delta_months = 0
	group by ys.subreddit, suri.creation_delta_months, account_type
	--order by subreddit, suri.creation_delta_months, account_type
)
select * from sub_foster_data






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
)

select
		vs.*, 
		usj.creation_delta_months as threshold_match_month
	from  valid_subs vs
	--left join s2_user_subreddit_joins usj on vs.subreddit = usj.subreddit and ys.activity_threshold = usj.account_num
	left join s2_user_subreddit_joins usj on vs.subreddit = usj.subreddit
	where creation_delta_months is not null
	and usj.account_num = 20






--
--
--



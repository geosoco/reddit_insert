



--
-- Find proper union of all the unique subreddits included in the mod & creator subs
--


select distinct subreddit from
	
(select distinct subreddit from subreddit_moderator_updates
union all 
select distinct subreddit from subreddit_creator_updates) a 





--
--
--

with creators as (
	select distinct subreddit, creator
	from subreddit_creator_updates
	where creator != '[deleted]'
),
mods as (
	select distinct subreddit, moderator
	from subreddit_moderator_updates
	where moderator != '[deleted]'
)
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
	where susd.total_months >= 3 and susd.total_submissions > (total_months * 10) 
	and susd.first_delta_month <= s.creation_delta_months and susd.last_delta_month >= s.creation_delta_months
	
	group by s.subreddit, s.creation_delta_months









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
)
	select
		susd.subreddit, 
		susd.author,
		sum(susd.total_months) as total_months,
		sum(total_activity) as total_activity,
		sum(total_submissions) as total_submissions,

		sum(case when c.creator is not null then 1 else 0 end) as is_creator,
		sum(case when m.moderator is not null then 1 else 0 end) as is_mod
	
	from s2_sub_user_sequence_data susd
	left join creators c on c.subreddit = susd.subreddit and c.creator = susd.author
	left join moderators m on m.subreddit = susd.subreddit and m.moderator = susd.author
	where susd.total_months >= 4 and susd.total_submissions > (total_months * 10)  and c.creator != '[deleted]' and m.moderator != '[deleted]'
	
	group by susd.subreddit, susd.author







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
)
	select
		susd.subreddit, 
		susd.author,
		susd.*,
		c.creator,
		m.moderator
--		sum(susd.total_months) as total_months,
--		sum(total_activity) as total_activity,
--		sum(total_submissions) as total_submissions,

--		sum(case when c.creator is not null then 1 else 0 end) as is_creator,
--		sum(case when m.moderator is not null then 1 else 0 end) as is_mod
	
	from s2_sub_user_sequence_data susd
	left join creators c on c.subreddit = susd.subreddit and c.creator = susd.author
	left join moderators m on m.subreddit = susd.subreddit and m.moderator = susd.author
	where susd.total_months >= 4 and susd.total_submissions > (total_months * 10)  and c.creator != '[deleted]' and m.moderator != '[deleted]' and susd.subreddit = 'AnOldGruntNews'







	with creators as (
	select distinct subreddit, creator
	from subreddit_creator_updates
	where creator != '[deleted]'
),
mods as (
	select subreddit, count(distinct moderator) as count_moderators
	from subreddit_moderator_updates
	where moderator != '[deleted]' and moderator is not null
	group by subreddit
),
foster as (
select
		s.subreddit, 
		s.creation_delta_months,
		susd.author,
		s.total_activity,
		s.total_submissions,
		s.total_comments,
		s.is_mod,
		case when c.creator = susd.author then 1 else 0 end as is_creator,
		case when c.creator is not null then 1 else 0 end as has_creator_data,
		case when m.subreddit is not null then 1 else 0 end as has_moderator_data
	
		
	from s2_sub_user_retention_intermediate s
	left join s2_sub_user_sequence_data susd on s.subreddit = susd.subreddit and s.author = susd.author
	left join creators c on c.subreddit = s.subreddit
	left join mods as m on m.subreddit = s.subreddit
	where susd.total_months >= 4 and susd.total_submissions > (total_months * 10) 
	and susd.first_delta_month <= s.creation_delta_months and susd.last_delta_month >= s.creation_delta_months
)
select
	is_creator, is_mod, count(*)
from foster
where has_moderator_data = 1 and has_creator_data = 1
group by is_creator, is_mod







--
-- count the number of creator/mod for each of the groups
--
-- Note: I did not try to find all, I think i did a stratefied sample
-- Note: creators are generally always mods, so the is_creator/is_mod isn't totally valid
-- While there are cases where that can happen, like a creator being removed, these were likely
-- situations where the moderators could not be found for the subreddit as they have *no mods*
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
		sum(total_submissions) as total_submissions
	
	from s2_sub_user_sequence_data susd
	where susd.total_months >= 4 and susd.total_submissions > (total_months * 10)  and susd.author != '[deleted]'
	
	group by susd.subreddit, susd.author

),
sub_foster2 as (
	select
		sfd.*,
		case when c.creator is not null then 1 else 0 end as is_creator,
		case when m.moderator is not null then 1 else 0 end as is_mod
		
	from sub_foster_data sfd
	left join creators c on c.subreddit = sfd.subreddit and c.creator = sfd.author
	left join moderators m on m.subreddit = sfd.subreddit and m.moderator = sfd.author	
	
)
-- foster as (
-- select
-- 		s.subreddit, 
-- 		s.creation_delta_months,
-- 		susd.author,
-- 		s.total_activity,
-- 		s.total_submissions,
-- 		s.total_comments,
-- 		s.is_mod,
-- 		case when c.creator = susd.author then 1 else 0 end as is_creator,
-- 		case when c.creator is not null then 1 else 0 end as has_creator_data,
-- 		case when m.subreddit is not null then 1 else 0 end as has_moderator_data
	
		
-- 	from s2_sub_user_retention_intermediate s
-- 	left join s2_sub_user_sequence_data susd on s.subreddit = susd.subreddit and s.author = susd.author
-- 	left join creators c on c.subreddit = s.subreddit
-- 	left join mods as m on m.subreddit = s.subreddit
-- 	where susd.total_months >= 4 and susd.total_submissions > (total_months * 10) 
-- 	and susd.first_delta_month <= s.creation_delta_months and susd.last_delta_month >= s.creation_delta_months
-- )
select
	is_creator, is_mod, count(*)
from sub_foster2 sf2
inner join mods_and_creator_subs macs on macs.subreddit = sf2.subreddit
where has_moderator IS TRUE or has_creator IS TRUE
group by is_creator, is_mod

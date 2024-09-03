--
-- s2_sub_user_retention_intermediate
--

drop table if exists s2_sub_user_retention_intermediate;


with year_subs as (
	select display_name, created_utc
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' 
),
sub_meta as (
	select distinct subreddit
	from subreddit_moderator_updates

	union

	select distinct subreddit
	from subreddit_creator_updates
),
sub_moderators as (
	select distinct subreddit, moderator
	from subreddit_moderator_updates
),
creators as (
	select distinct subreddit, creator
	from subreddit_creator_updates
	where creator != '[deleted]'
),
lead_data as (
	select
		author,
		usa.subreddit, 
		creation_delta_months,
		lead(creation_delta_months) over (partition by usa.subreddit, author order by creation_delta_months asc) as next_active_month,
		lag(creation_delta_months) over (partition by usa.subreddit, author order by creation_delta_months asc) as prev_active_month,
		case when lead(creation_delta_months) over (partition by usa.subreddit, author order by creation_delta_months asc) = creation_delta_months+1 then 1 else 0 end as active_next_month,
		case when lag(creation_delta_months) over (partition by usa.subreddit, author order by creation_delta_months asc) = creation_delta_months-1 then 1 else 0 end as active_prev_month,
		case when sm2.moderator is not null then 1 else 0 end as  is_mod,
		case when c.creator is not null then 1 else 0 end as is_creator,
		total_activity,
		total_submissions,
		total_comments
	from year_subs ys
	left join user_sub_activity_30day_activity usa on usa.subreddit = ys.display_name
	left join sub_moderators sm2 on sm2.subreddit = ys.display_name and sm2.moderator = usa.author
	left join creators c on ys.display_name = c.subreddit and c.creator = usa.author
)
select * 
into s2_sub_user_retention_intermediate	
from lead_data;



grant select on s2_sub_user_retention_intermediate to public;


create index on s2_sub_user_retention_intermediate(author);
create index on s2_sub_user_retention_intermediate(author, subreddit);
create index on s2_sub_user_retention_intermediate(subreddit);



drop table if exists s3_sub_user_sequence_data2;


with year_subs as (
	select display_name, created_utc
	from subreddits
	where display_name in ('hillaryclinton', 'SandersForPresident', 'The_Donald')
),
sub_meta as (
	select distinct subreddit
	from s3_moderator_updates

	union

	select distinct subreddit
	from s3_creator_updates
),
sub_moderators as (
	select distinct subreddit, moderator
	from subreddit_moderator_updates
),
creators as (
	select distinct subreddit, creator
	from s3_creator_updates
	where creator != '[deleted]'
),
sub_user_monthly_retention_intermediate as (
	select
		author,
		subreddit, 
		creation_delta_months,
		lead(creation_delta_months) over w as next_active_month,
		case when lead(creation_delta_months) over w = creation_delta_months +  1 then 1 else 0 end as active_next_month,
		case when lag(creation_delta_months) over w = creation_delta_months - 1 then 1 else 0 end as active_prev_month,
		case when lead(creation_delta_months) over w = creation_delta_months +  1 then lead(total_activity) over w else 0 end as next_month_activity,
		case when lag(creation_delta_months) over w = creation_delta_months - 1 then lag(total_activity) over w else 0 end as prev_month_activity,
		total_activity,
		total_submissions,
		total_comments
	from year_subs ys
	left join user_sub_activity_30day_activity usa on usa.subreddit = ys.display_name
	where author != '[deleted]'	 
	window w as (partition by subreddit, author order by creation_delta_months asc)
),
seq_data as (
	-- The numbers below can be one so long as the data above filters to the proper threshold
	-- meaning that this will already only consider rows that are 

	select
		*, 
		case when prev_month_activity < 5 and total_activity >= 5 then 1 else NULL end as seq_start,
		case when total_activity >= 5 and next_month_activity < 5 then 1 else NULL end as seq_end
	
		from sub_user_monthly_retention_intermediate
		where author != '[deleted]' 
),
boundaries_table as (
	select 
		*,

		case when total_activity < 5 then null else
		0 + sum(seq_start) over (partition by subreddit, author order by creation_delta_months) 
		end	as seq_id
	
	from seq_data
)
select
	subreddit, author, seq_id, 
	min(case when seq_start = 1 then creation_delta_months else NULL end) as first_delta_month,
	max(case when seq_end = 1 then creation_delta_months else NULL end) as last_delta_month,
	count(*) as total_months,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
	
into s3_sub_user_sequence_data2
from boundaries_table
where seq_id is not null
group by subreddit, author, seq_id
order by subreddit, author, seq_id;


grant select on s3_sub_user_sequence_data2 to public;

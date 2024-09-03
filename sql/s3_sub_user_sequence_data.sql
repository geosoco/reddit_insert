--
-- s3_sub_user_sequence_data
--

drop table if exists s3_sub_user_sequence_data;

with boundaries_table as (
	select 
		*,

		case when active_prev_month = 0 and active_next_month = 0 then null else
		0 + sum(case when active_prev_month = 0 and active_next_month = 1 then 1 else 0 end) over (partition by subreddit, author order by creation_delta_months) 
		end	as seq_id
	
	from 
	(select
		*, 
		case when active_prev_month = 0 and active_next_month = 1 then 1 else NULL end as seq_start,
		case when active_prev_month = 1 and active_next_month = 0 then 1 else NULL end as seq_end
	
		from s3_sub_user_retention_intermediate
		where author != '[deleted]'
	) a
)
select
	subreddit, author, seq_id, 
	max(is_mod) as is_mod,
	max(is_creator) as is_creator,
	min(case when seq_start = 1 then creation_delta_months else NULL end) as first_delta_month,
	max(case when seq_end = 1 then creation_delta_months else NULL end) as last_delta_month,
	count(*) as total_months,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
	
into s3_sub_user_sequence_data
from boundaries_table
where seq_id is not null
group by subreddit, author, seq_id
order by subreddit, author, seq_id;



grant select on s3_sub_user_sequence_data to public;





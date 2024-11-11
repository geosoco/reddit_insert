--
--
-- s2_sub_user_sequence_data2
--
--
-- This table focuses on new fostering definition that requires a minimum amount of participation for each month
-- as the other table only considers periods of full retention.
--
--


drop table if exists  s2_sub_user_sequence_data2;



with boundaries_table as (
select 
		*,
		case when total_activity < 10 then NULL else
		0 + sum(seq_start) over (partition by subreddit, author order by creation_delta_months)  end as seq_id
	
	from 
	(select
		*, 
		case when (prev_total_activity < 10 or prev_total_activity is null) and total_activity >= 10 then 1 else NULL end as seq_start,
		case when total_activity >= 10 and (next_total_activity < 10 or next_total_activity is null) then 1 else NULL end as seq_end
	
		from s2_user_sub_retentive_activity_full
		where author != '[deleted]'
	) a
)

select
	subreddit, author, seq_id, 
	min(case when seq_start = 1 then creation_delta_months else NULL end) as first_delta_month,
	max(case when seq_end = 1 then creation_delta_months else NULL end) as last_delta_month,
	count(*) as total_months,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
	
into s2_sub_user_sequence_data2
from boundaries_table
where seq_id is not null
group by subreddit, author, seq_id
order by subreddit, author, seq_id;



grant select on s2_sub_user_sequence_data2 to public;

create index on s2_sub_user_sequence_data2(author);
create index on s2_sub_user_sequence_data2(subreddit);
create index on s2_sub_user_sequence_data2(subreddit, author);
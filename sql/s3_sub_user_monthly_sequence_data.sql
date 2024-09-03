--
-- s3_sub_user_monthly_sequence_data
--
-- like the 30-day user_sequence_data, but based on actual months
--

drop table if exists s3_sub_user_monthly_sequence_data;

with 
sub_user_monthly_retention_intermediate as (
	select
		author,
		subreddit, 
		month_year,
		lead(month_year) over (partition by subreddit, author order by month_year asc) as next_active_month,
		case when lead(month_year) over (partition by subreddit, author order by month_year asc) = month_year + interval '1' month then 1 else 0 end as active_next_month,
		case when lag(month_year) over (partition by subreddit, author order by month_year asc) = month_year - interval '1' month then 1 else 0 end as active_prev_month,
		total_activity,
		total_submissions,
		total_comments
	from s3_user_sub_activity_monthly_activity
	where author != '[deleted]'		
),
seq_data as (
	select
		*, 
		case when active_prev_month = 0 and active_next_month = 1 then 1 else NULL end as seq_start,
		case when active_prev_month = 1 and active_next_month = 0 then 1 else NULL end as seq_end
	
		from sub_user_monthly_retention_intermediate
		where author != '[deleted]'
),
boundaries_table as (
	select 
		*,

		case when active_prev_month = 0 and active_next_month = 0 then null else
		0 + sum(case when active_prev_month = 0 and active_next_month = 1 then 1 else 0 end) over (partition by subreddit, author order by month_year) 
		end	as seq_id
	
	from seq_data
)
select
	subreddit, author, seq_id, 
	min(case when seq_start = 1 then month_year else NULL end) as first_delta_month,
	max(case when seq_end = 1 then month_year else NULL end) as last_delta_month,
	count(*) as total_months,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
	
into s3_sub_user_monthly_sequence_data
from boundaries_table
where seq_id is not null
group by subreddit, author, seq_id
order by subreddit, author, seq_id;



grant select on s3_sub_user_monthly_sequence_data to public;



create index on s3_sub_user_monthly_sequence_data (subreddit);
create index on s3_sub_user_monthly_sequence_data (author);
create index on s3_sub_user_monthly_sequence_data using btree (total_months);
create index on s3_sub_user_monthly_sequence_data using btree (total_submissions);
create index on s3_sub_user_monthly_sequence_data using btree (total_activity);
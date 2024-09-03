--
-- sub_user_retention_intermediate
--

drop table if exists sub_user_retention_intermediate;


with subreddit_list as (
	select name from subreddit_summary
	where total_activity >= 1000 and unique_authors >= 10
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
		total_activity,
		total_submissions,
		total_comments
	from subreddit_list sl
	left join user_sub_activity_30day_activity usa on sl.name = usa.subreddit

)
select * 
into sub_user_retention_intermediate	
from lead_data;



grant select on sub_user_retention_intermediate to public;


create index on sub_user_retention_intermediate(author);
create index on sub_user_retention_intermediate(author, subreddit);
create index on sub_user_retention_intermediate(subreddit);
	
	
	

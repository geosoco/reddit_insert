--
-- s3_user_sub_activity_monthly_activity
--
-- This table is used to create the retention/turnover table
-- There are some intermediate analyses that could use this data, but 
-- otherwise could be a temp table just to create turnover table
--

drop table if exists s3_user_sub_activity_monthly_activity;


with sub_creation as (
	select display_name, created_utc
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
)
select
	uca.subreddit,
	uca.author, 
	date_trunc('month', uca.created_utc) as month_year,
	count(*) as total_activity,
	sum(case when c_id is not null then 1 else 0 end) as total_comments,
	sum(case when s_id is not null then 1 else 0 end) as total_submissions

into s3_user_sub_activity_monthly_activity
from sub_creation sc
left join user_combined_activity uca on sc.display_name = uca.subreddit
group by uca.subreddit, uca.author, month_year;


grant select on s3_user_sub_activity_monthly_activity to public;
	
create index on s3_user_sub_activity_monthly_activity(subreddit);
create index on s3_user_sub_activity_monthly_activity(author);


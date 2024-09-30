--
-- s3_subreddit_monthly_retention
--


drop table if exists s3_subreddit_monthly_retention;

--select subreddit, author, month_year, month_year + interval '1' month from s3_user_sub_activity_monthly_activity limit 10;


with subs_creation as (
	select display_name, created_utc
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
),
lead_data as (
select
	author,
	subreddit, 
	month_year,
	lead(month_year) over (partition by subreddit, author order by month_year asc) as next_active_month,
	case when lead(month_year) over (partition by subreddit, author order by month_year asc) = month_year + interval '1' month then 1 else 0 end as active_next_month,
	total_activity,
	total_submissions,
	total_comments
from s3_user_sub_activity_monthly_activity
where author != '[deleted]'
)

select
	subreddit, month_year, 
	count(*) as total_active_authors, 
	sum(active_next_month) as active_next_month,
	(sum(case when month_year+ interval '1' month = next_active_month then 1 else 0 end) * 100.0 / count(*)) as retention_rate,
	100.0 - (sum(case when month_year+ interval '1' month  = next_active_month then 1 else 0 end) * 100.0 / count(*)) as turnover_rate,
	sum(total_activity) as total_activity,
	sum(total_submissions) as total_submissions,
	sum(total_comments) as total_comments
into s3_subreddit_monthly_retention		
from lead_data
group by subreddit, month_year
order by subreddit, month_year;


grant select on s3_subreddit_monthly_retention to public;

create index on s3_subreddit_monthly_retention(subreddit);


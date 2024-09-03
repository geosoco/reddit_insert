-- Calculate gini data (submission only) for active subreddits by year
-- takes about 4 hours and produces like 97 rows. lol
-- *******
-- NOTE: This calculation removes AutoModerator, which changes the gini for several calculations
-- ******
-- depends: subreddit_summary, user_subreddit_daily_summary
-- exceptions: this excludes '[deleted]' as a user. focuses on 

drop table if exists s3_subreddit_gini_month;

with 
subreddit_list as (
	select name from 
	(values ('hillaryclinton'), ('The_Donald'), ('SandersForPresident')) as t (name)

),
raw_data as (
	select subreddit, 
		date_trunc('month', date) as month_year,
		author,
		num_submissions,
		num_comments,
		total_items as total_activity
	from subreddit_list sl
	left join user_subreddit_daily_summary usds on usds.subreddit = sl.name
	where author not in ('[deleted]', 'AutoModerator')
),
summarized as (
	select subreddit, author, month_year, 
		sum(num_submissions) as num_submissions,
		sum(num_comments) as num_comments,
		sum(total_activity) as total_activity
	from raw_data
	group by subreddit, author, month_year
),
ranked_submission_data as (
	select 
		subreddit, 
		author,
		month_year,
		num_submissions, 
		row_number() over (partition by subreddit, month_year order by num_submissions) as sub_submissions_rank
	from summarized
	where num_submissions > 0	
),
ranked_comment_data as (
	select 
		subreddit, 
		author,
		month_year, 
		num_comments, 
		row_number() over (partition by subreddit, month_year order by num_comments) as sub_comments_rank
	from summarized
	where num_comments > 0	
	
),
ranked_activity_data as (
	select 
		subreddit, 
		author,
		month_year, 
		total_activity, 
		row_number() over (partition by subreddit, month_year order by total_activity) as sub_activity_rank
	from summarized
	where total_activity > 0	

)

select
	rad.subreddit,
	rad.month_year::timestamp without time zone, 
	sum(rad.total_activity) as total_activity,
	sum(num_comments) as total_comments,
	sum(num_submissions) as total_submissions,
	count(distinct rad.author) as unique_activity_authors,
	count(distinct rcd.author) filter(where num_comments>0) as unique_comments_authors,
	count(distinct rsd.author) filter(where num_submissions>0) as unique_submissions_authors,
	((2.0 * sum(total_activity * sub_activity_rank)/sum(total_activity)) - (MAX(sub_activity_rank)+1.0))/MAX(sub_activity_rank) as total_activity_gini,
	((2.0 * sum(num_comments * sub_comments_rank)/sum(num_comments)) - (MAX(sub_comments_rank)+1.0))/MAX(sub_comments_rank) as total_comments_gini,
	((2.0 * sum(num_submissions * sub_submissions_rank)/sum(num_submissions)) - (MAX(sub_submissions_rank)+1.0))/MAX(sub_submissions_rank) as total_submissions_gini

	into table s3_subreddit_gini_month
	from ranked_activity_data rad
	left join ranked_comment_data rcd on rcd.subreddit = rad.subreddit and rcd.author = rad.author and rcd.month_year = rad.month_year
	left join ranked_submission_data rsd on rsd.subreddit = rad.subreddit and rsd.author = rad.author and rsd.month_year = rad.month_year
	group by rad.subreddit, rad.month_year;


grant select on s3_subreddit_gini_month to public;



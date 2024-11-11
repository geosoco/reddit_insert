--------------------------------------------------------------
--
-- s2_subreddit_gini_30days
--
--------------------------------------------------------------


-- Calculate gini data (submission only) for active subreddits
-- takes about 1 hour
-- depends: subreddit_summary, user_subreddit_daily_summary
-- exceptions: this excludes '[deleted]' as a user. focuses on 


drop table if exists s2_subreddit_gini_30days;


with year_subs as (
	select 
		display_name, 
		created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
),
raw_data as (
	select subreddit, 
		(extract(epoch from (usds.date - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
		author,
		num_submissions,
		num_comments,
		total_items as total_activity
	from year_subs ys
	left join user_subreddit_daily_summary usds on usds.subreddit = ys.display_name
	where author not in ('[deleted]', 'AutoModerator')
),
summarized as (
	select subreddit, author, creation_delta_months, 
		sum(num_submissions) as num_submissions,
		sum(num_comments) as num_comments,
		sum(total_activity) as total_activity
	from raw_data
	group by subreddit, author, creation_delta_months
),
ranked_submission_data as (
	select 
		subreddit, 
		author,
		creation_delta_months,
		num_submissions, 
		row_number() over (partition by subreddit, creation_delta_months order by num_submissions) as sub_submissions_rank
	from summarized
	where num_submissions > 0	
),
ranked_comment_data as (
	select 
		subreddit, 
		author,
		creation_delta_months, 
		num_comments, 
		row_number() over (partition by subreddit, creation_delta_months order by num_comments) as sub_comments_rank
	from summarized
	where num_comments > 0	
	
),
ranked_activity_data as (
	select 
		subreddit, 
		author,
		creation_delta_months, 
		total_activity, 
		row_number() over (partition by subreddit, creation_delta_months order by total_activity) as sub_activity_rank
	from summarized
	where total_activity > 0	

)

select
	rad.subreddit,
	rad.creation_delta_months, 
	sum(rad.total_activity) as total_activity,
	sum(num_comments) as total_comments,
	sum(num_submissions) as total_submissions,
	count(distinct rad.author) as unique_activity_authors,
	count(distinct rcd.author) filter(where num_comments>0) as unique_comments_authors,
	count(distinct rsd.author) filter(where num_submissions>0) as unique_submissions_authors,
	((2.0 * sum(total_activity * sub_activity_rank)/sum(total_activity)) - (MAX(sub_activity_rank)+1.0))/MAX(sub_activity_rank) as total_activity_gini,
	((2.0 * sum(num_comments * sub_comments_rank)/sum(num_comments)) - (MAX(sub_comments_rank)+1.0))/MAX(sub_comments_rank) as total_comments_gini,
	((2.0 * sum(num_submissions * sub_submissions_rank)/sum(num_submissions)) - (MAX(sub_submissions_rank)+1.0))/MAX(sub_submissions_rank) as total_submissions_gini

	into table s2_subreddit_gini_30days
	from ranked_activity_data rad
	left join ranked_comment_data rcd on rcd.subreddit = rad.subreddit and rcd.author = rad.author and rcd.month_year = rad.month_year
	left join ranked_submission_data rsd on rsd.subreddit = rad.subreddit and rsd.author = rad.author and rsd.month_year = rad.month_year
	group by rad.subreddit, rad.month_year;


grant select on s2_subreddit_gini_30days to public;




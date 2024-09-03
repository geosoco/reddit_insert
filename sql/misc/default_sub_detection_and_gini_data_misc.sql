-- misc queries before a restart


select id, created_utc, author, subreddit, data->>'url', data->>'title', data->>'selftext', data from submissions
where subreddit in ('announcements', 'blog', 'changelog')
and created_utc < '2011-10-18'
and data->>'selftext' ~* '(front page|subscrib|default|personaliz|frontpage|customi|subreddits)'


--

select *,  from submissions
where subreddit = 'space'
and created_utc < '2014-05-09'
and (data->>'title' ~* 'default' or 
	data->>'selftext' ~* 'default'
	)


--



-- Calculate gini data (submission only) for active subreddits by year
-- takes about 1 hour
-- depends: subreddit_summary, user_subreddit_daily_summary
-- exceptions: this excludes '[deleted]' as a user. focuses on 

drop table if exists subreddit_gini_defaults_month;

with 
subreddit_list as (
	select name from 
	(values ('pics'), ('gaming'), ('worldnews'), ('videos'), ('todayilearned'), ('IAmA'), ('funny'), ('atheism'), ('politics'), ('science'), ('AskReddit'), ('technology'), ('WTF'), ('bestof'), ('AdviceAnimals'), ('Music'), ('aww'), ('askscience'), ('movies'), ('books'), ('earthporn'), ('explainlikeimfive'), ('gifs'), ('news'), ('television'), ('Art'), ('creepy'), ('dataisbeautiful'), ('DIY'), ('Documentaries'), ('EarthPorn'), ('Fitness'), ('food'), ('Futurology'), ('gadgets'), ('GetMotivated'), ('history'), ('InternetIsBeautiful'), ('Jokes'), ('LifeProTips'), ('listentothis'), ('mildlyinteresting'), ('nosleep'), ('nottheonion'), ('OldSchoolCool'), ('personalfinance'), ('philosophy'), ('photoshopbattles'), ('Showerthoughts'), ('space'), ('sports'), ('tifu'), ('TwoXChromosomes'), ('UpliftingNews'), ('WritingPrompts')) as t (name)

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

	into table subreddit_gini_defaults_month
	from ranked_activity_data rad
	left join ranked_comment_data rcd on rcd.subreddit = rad.subreddit and rcd.author = rad.author and rcd.month_year = rad.month_year
	left join ranked_submission_data rsd on rsd.subreddit = rad.subreddit and rsd.author = rad.author and rsd.month_year = rad.month_year
	group by rad.subreddit, rad.month_year;


grant select on subreddit_gini_defaults_month to public;


-- select
-- 	rad.subreddit,
-- 	rad.author,
-- 	rad.total_activity,
-- 	sub_activity_rank,
-- 	num_comments,
-- 	sub_comments_rank,
-- 	num_submissions,
-- 	sub_submissions_rank
-- 	from ranked_activity_data rad
-- 	left join ranked_comment_data rcd on rcd.subreddit = rad.subreddit and rcd.author = rad.author
-- 	left join ranked_submission_data rsd on rsd.subreddit = rad.subreddit and rsd.author = rad.author
-- 	limit 10000


-- semi_totals as (
-- 	select
-- 		subreddit,
-- 		author,
-- 		sum(num_submissions) as total_submissions,
-- 		sum(num_comments) as total_comments,
-- 		sum(total_activity) as total_activity,
-- 		count(*) filter(where num_submissions > 0) over () as count_submissions,
-- 		count(*) filter(where num_comments >0) as count_comments,
-- 		count(*) filter(where total_activity >0) as count_total_activity,
-- 		count(distinct author) filter(where num_submissions > 0) as unique_authors_submissions,
-- 		count(distinct author) filter(where num_comments > 0) as unique_authors_comments,
-- 		count(distinct author) filter(where total_activity > 0) as unique_authors_total_activity,
	
-- 	from ranked_data
-- 	group by subreddit
-- 	order by subreddit
-- )
-- select
-- 	subreddit,
-- 	total_submissions,
-- 	total_comments,
-- 	total_activity,
-- 	count_submissions,
-- 	count_comments,
-- 	count_total_activity,
-- 	unique_authors_submissions,
-- 	unique_authors_comments,
-- 	unique_authors_total_activity,
-- 	((2.0 * sum(total_submissions * rank )/sum(total_submissions)) - (count_submissions+1.0))/count_submissions as gini_submissions,
-- 	((2.0 * sum(total_comments * rank )/sum(total_submissions)) - (count_submissions+1.0))/count_submissions as gini_comments,
-- 	((2.0 * sum(total_submissions * rank )/sum(total_submissions)) - (count_submissions+1.0))/count_submissions as gini_submissions,
	
-- into table subreddit_gini_submissions
-- from semi_totals
-- group by subreddit

-- select
-- 	rad.subreddit,
-- 	sum(rad.total_activity) as total_activity,
-- 	count(distinct rad.author) as unique_activity_authors,
-- 	sum(num_comments) as total_comments,
-- 	count(distinct rcd.author) filter(where num_comments>0) as unique_comments_authors,
-- 	sum(num_submissions) as total_submissions,
-- 	count(distinct rsd.author) filter(where num_submissions>0) as unique_submissions_authors,
	
-- 	json_agg(json_build_object(rad.author, rad.total_activity)) filter(where total_activity>0) as all_users,
-- 	json_agg(json_build_object(rsd.author, rsd.num_submissions)) filter(where num_submissions>0) as submitters,
-- 	json_agg(json_build_object(rcd.author, rcd.num_comments)) filter(where num_comments>0) as commenters
	
-- 	from ranked_activity_data rad
-- 	left join ranked_comment_data rcd on rcd.subreddit = rad.subreddit and rcd.author = rad.author
-- 	left join ranked_submission_data rsd on rsd.subreddit = rad.subreddit and rsd.author = rad.author	
-- 	group by rad.subreddit


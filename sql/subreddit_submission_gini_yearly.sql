-- Calculate yearly gini data (submission only) for active subreddits
-- takes about 1 hour
-- depends: subreddit_summary, user_subreddit_daily_summary
-- exceptions: this excludes '[deleted]' as a user. focuses on 

with 
subreddit_list as (
	select name from subreddit_summary
	where total_submissions >= 1000 and unique_authors >= 5
),
raw_data as (
	select subreddit, 
		extract(year from date) as year,  
		author,
		num_submissions
	from subreddit_list sl
	left join user_subreddit_daily_summary usds on usds.subreddit = sl.name
	where author != '[deleted]' and num_submissions != 0
),
summarized as (
	select subreddit, year, author, sum(num_submissions) as num_submissions
	from raw_data
	group by subreddit, year, author
	order by num_submissions asc
),
ranked_data as (
	select 
		subreddit, 
		year,
		author,
		num_submissions, 
		row_number() over (partition by subreddit,year order by num_submissions) as rank
	from summarized
	where num_submissions > 0
	order by rank asc
)
select
	subreddit,
	year,
	sum(num_submissions) as total_submissions,
	count(distinct author) as unique_authors,
	((2.0 * sum(num_submissions * rank )/sum(num_submissions)) - (count(*)+1.0))/count(*) as gini
into table subreddit_gini_year
from ranked_data
group by subreddit, year
order by subreddit, year

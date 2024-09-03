-- Calculate yearly gini data (submission only) for active subreddits
-- takes about 1 hour
-- depends: subreddit_summary, user_subreddit_daily_summary
-- exceptions: this excludes '[deleted]' as a user. focuses on 

drop table if exists subreddit_gini_defaults_submissions_month;


with 
subreddit_list as (
	select name from 
	(values ('pics'), ('gaming'), ('worldnews'), ('videos'), ('todayilearned'), ('IAmA'), ('funny'), ('atheism'), ('politics'), ('science'), ('AskReddit'), ('technology'), ('WTF'), ('bestof'), ('AdviceAnimals'), ('Music'), ('aww'), ('askscience'), ('movies'), ('books'), ('earthporn'), ('explainlikeimfive'), ('gifs'), ('news'), ('television'), ('Art'), ('creepy'), ('dataisbeautiful'), ('DIY'), ('Documentaries'), ('EarthPorn'), ('Fitness'), ('food'), ('Futurology'), ('gadgets'), ('GetMotivated'), ('history'), ('InternetIsBeautiful'), ('Jokes'), ('LifeProTips'), ('listentothis'), ('mildlyinteresting'), ('nosleep'), ('nottheonion'), ('oldschoolcool'), ('personalfinance'), ('philosophy'), ('photoshopbattles'), ('Showerthoughts'), ('space'), ('sports'), ('tifu'), ('TwoXChromosomes'), ('UpliftingNews'), ('writingprompts')) as t (name)
),
raw_data as (
	select subreddit, 
		date_trunc('month', date) as month_year,
		author,
		num_submissions
	from subreddit_list sl
	left join user_subreddit_daily_summary usds on usds.subreddit = sl.name
	where author != '[deleted]' and num_submissions != 0
),
summarized as (
	select subreddit, month_year, author, sum(num_submissions) as num_submissions
	from raw_data
	group by subreddit, month_year, author
	order by num_submissions asc
),
ranked_data as (
	select 
		subreddit, 
		month_year,
		author,
		num_submissions, 
		row_number() over (partition by subreddit,month_year order by num_submissions) as rank
	from summarized
	where num_submissions > 0
	order by rank asc
)
select
	subreddit,
	month_year::timestamp without time zone,
	sum(num_submissions) as total_submissions,
	count(distinct author) as unique_authors,
	((2.0 * sum(num_submissions * rank )/sum(num_submissions)) - (count(*)+1.0))/count(*) as gini
into table subreddit_gini_defaults_submissions_month
from ranked_data
group by subreddit, month_year
order by subreddit, month_year


grant select on subreddit_gini_defaults_submissions_month to public;

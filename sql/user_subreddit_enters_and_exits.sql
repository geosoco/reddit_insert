-- enters

with user_sub_data as (select
	subreddit,
	date_trunc('day', first_activity_time)::timestamp without time zone as date,
	first_activity_time,
	last_activity_time,
	total_activity,
	total_submissions,
	total_comments,
	extract(day from last_activity_time - first_activity_time) as num_days
from user_subreddit_activity
where subreddit in ('pics','gaming','worldnews','videos','todayilearned','IAmA','funny','atheism','politics','science','AskReddit','technology','WTF','bestof','AdviceAnimals','Music','aww','askscience','movies','books','EarthPorn','explainlikeimfive','gifs','news','television','Art','creepy','dataisbeautiful','DIY','Documentaries','Fitness','food','Futurology','gadgets','GetMotivated','history','InternetIsBeautiful','Jokes','LifeProTips','listentothis','mildlyinteresting','nosleep','nottheonion','OldSchoolCool','personalfinance','philosophy','photoshopbattles','Showerthoughts','space','sports','tifu','TwoXChromosomes','UpliftingNews','WritingPrompts')
),
timediff_buckets as (
	select
		*,
		case when num_days < 1 then '1d'
			when num_days < 30 then '30d'
			when num_days < 90 then '90d'
			when num_days < 180 then '180d'
			when num_days < 365 then '365d'
			when num_days >= 365 then '1y'
		end as time_bucket
	from user_sub_data
)
select
	subreddit,
	date,
	time_bucket,
	count(*)
into user_subreddit_enters
from timediff_buckets
group by subreddit, date, time_bucket
order by subreddit, date asc;

grant select on user_subreddit_enters to public;
	

------------------------
--
-- exits
--
------------------------

drop table if exists user_subreddit_exits;

with user_sub_data as (select
	subreddit,
	date_trunc('day', last_activity_time)::timestamp without time zone as date,
	first_activity_time,
	last_activity_time,
	total_activity,
	total_submissions,
	total_comments,
	extract(day from last_activity_time - first_activity_time) as num_days
from user_subreddit_activity
where subreddit in ('pics','gaming','worldnews','videos','todayilearned','IAmA','funny','atheism','politics','science','AskReddit','technology','WTF','bestof','AdviceAnimals','Music','aww','askscience','movies','books','EarthPorn','explainlikeimfive','gifs','news','television','Art','creepy','dataisbeautiful','DIY','Documentaries','Fitness','food','Futurology','gadgets','GetMotivated','history','InternetIsBeautiful','Jokes','LifeProTips','listentothis','mildlyinteresting','nosleep','nottheonion','OldSchoolCool','personalfinance','philosophy','photoshopbattles','Showerthoughts','space','sports','tifu','TwoXChromosomes','UpliftingNews','WritingPrompts')
),
timediff_buckets as (
	select
		*,
		case when num_days < 1 then '1d'
			when num_days < 30 then '30d'
			when num_days < 90 then '90d'
			when num_days < 180 then '180d'
			when num_days < 365 then '365d'
			when num_days >= 365 then '1y'
		end as time_bucket
	from user_sub_data
)
select
	subreddit,
	date,
	time_bucket,
	count(*)
into user_subreddit_enters
from timediff_buckets
group by subreddit, date, time_bucket
order by subreddit, date asc;

grant select on user_subredit_exits to public;

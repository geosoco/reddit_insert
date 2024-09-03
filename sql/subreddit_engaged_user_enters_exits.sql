drop table if exists subreddit_engaged_user_enters_exits;

with engaged_users as (
select *,
	date_trunc('day', first_activity_time)::timestamp without time zone as enter_date,
	date_trunc('day', last_activity_time)::timestamp without time zone as exit_date,
	extract(epoch from last_activity_time) - extract(epoch from first_activity_time) as tenure
	from user_subreddit_activity
where total_activity >= 5 and author != '[deleted]'
	and subreddit in ('pics','gaming','worldnews','videos','todayilearned','IAmA','funny','atheism','politics','science','AskReddit','technology','WTF','bestof','AdviceAnimals','Music','aww','askscience','movies','books','EarthPorn','explainlikeimfive','gifs','news','television','Art','creepy','dataisbeautiful','DIY','Documentaries','Fitness','food','Futurology','gadgets','GetMotivated','history','InternetIsBeautiful','Jokes','LifeProTips','listentothis','mildlyinteresting','nosleep','nottheonion','OldSchoolCool','personalfinance','philosophy','photoshopbattles','Showerthoughts','space','sports','tifu','TwoXChromosomes','UpliftingNews','WritingPrompts')
),
enters as (
	select 
		subreddit, 
		enter_date as date,
		count(*) as engaged_enters,
		avg(tenure) as avg_enter_tenure,
		percentile_cont(0.5) within group(order by tenure) as median_enter_tenure
	from engaged_users
	group by subreddit, enter_date
),
exits as (
	select 
		subreddit, 
		exit_date as date,
		count(*) as engaged_exits,
		avg(tenure) as avg_exit_tenure,
		percentile_cont(0.5) within group(order by tenure) as median_exit_tenure
	
	from engaged_users
	group by subreddit, exit_date	
)
select
	coalesce(en.subreddit, ex.subreddit) as subreddit,
	coalesce(en.date, ex.date) as date,
	coalesce(en.engaged_enters, 0) as engaged_enters,
	coalesce(ex.engaged_exits, 0) as engaged_exits,
	coalesce(en.avg_enter_tenure, 0) as avg_enter_tenure,
	coalesce(en.median_enter_tenure, 0) as median_enter_tenure,
	coalesce(ex.avg_exit_tenure, 0) as avg_exit_tenure,
	coalesce(ex.median_exit_tenure, 0) as median_exit_tenure
	into subreddit_engaged_user_enters_exits
	from enters en
	full outer join exits ex on (en.subreddit = ex.subreddit and en.date = ex.date)
	order by subreddit, date asc;
	
	
	
grant select on subreddit_engaged_user_enters_exits to public;



#
# 
# with meta
#
#
#





drop table if exists subreddit_engaged_user_enters_exits_with_meta;

with engaged_users as (
select *,
	date_trunc('day', first_activity_time)::timestamp without time zone as enter_date,
	date_trunc('day', last_activity_time)::timestamp without time zone as exit_date,
	extract(epoch from last_activity_time) - extract(epoch from first_activity_time) as tenure
	from user_subreddit_activity
where total_activity >= 5 and author != '[deleted]'
	and subreddit in ('pics','gaming','worldnews','videos','todayilearned','IAmA','funny','atheism','politics','science','AskReddit','technology','WTF','bestof','AdviceAnimals','Music','aww','askscience','movies','books','EarthPorn','explainlikeimfive','gifs','news','television','Art','creepy','dataisbeautiful','DIY','Documentaries','Fitness','food','Futurology','gadgets','GetMotivated','history','InternetIsBeautiful','Jokes','LifeProTips','listentothis','mildlyinteresting','nosleep','nottheonion','OldSchoolCool','personalfinance','philosophy','photoshopbattles','Showerthoughts','space','sports','tifu','TwoXChromosomes','UpliftingNews','WritingPrompts')
),
enters as (
	select 
		subreddit, 
		enter_date as date,
		count(*) as engaged_enters,
		avg(tenure) as avg_enter_tenure,
		percentile_cont(0.5) within group(order by tenure) as median_enter_tenure,
		avg(total_submissions) as avg_submissions,
		avg(total_comments) as total_comments,
		avg(total_submissions::numeric/(total_activity::numeric)) as avg_submission_ratio
	from engaged_users
	group by subreddit, enter_date
),
exits as (
	select 
		subreddit, 
		exit_date as date,
		count(*) as engaged_exits,
		avg(tenure) as avg_exit_tenure,
		percentile_cont(0.5) within group(order by tenure) as median_exit_tenure,
		avg(total_submissions) as avg_submissions,
		avg(total_comments) as total_comments,
		avg(total_submissions::numeric/(total_activity::numeric)) as avg_submission_ratio
	from engaged_users
	group by subreddit, exit_date	
)
select
	coalesce(en.subreddit, ex.subreddit) as subreddit,
	coalesce(en.date, ex.date) as date,
	coalesce(en.engaged_enters, 0) as engaged_enters,
	coalesce(ex.engaged_exits, 0) as engaged_exits,
	coalesce(en.avg_enter_tenure, 0) as avg_enter_tenure,
	coalesce(en.median_enter_tenure, 0) as median_enter_tenure,
	coalesce(ex.avg_exit_tenure, 0) as avg_exit_tenure,
	coalesce(ex.median_exit_tenure, 0) as median_exit_tenure
	into subreddit_engaged_user_enters_exits_with_meta
	from enters en
	full outer join exits ex on (en.subreddit = ex.subreddit and en.date = ex.date)
	order by subreddit, date asc;
	
	
	
grant select on subreddit_engaged_user_enters_exits_with_meta to public;


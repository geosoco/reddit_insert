
--
--
--

select
	dss.subreddit,
	extract(month from age(date_trunc('month', created_utc)::timestamp without time zone, date_trunc('month', dsm.added))) +
	(extract(year from age(date_trunc('month', created_utc)::timestamp without time zone, date_trunc('month', dsm.added))) * 12)
			as rel_month,
	count(*) as submission_count,
	count(*) FILTER (where author = 'AutoModerator') as submissions_automod,
	count(*) FILTER (where num_comments = 0) as comments_none,
	count(*) FILTER (where num_comments = 1) as comments_one,
	count(*) FILTER (where num_comments > 1) as comments_gt1,
	
	count(*) FILTER (where score < 0) as score_neg,
	count(*) FILTER (where score = 0) as score_0,
	count(*) FILTER (where score = 1) as score_1,
	count(*) FILTER (where score > 1) as score_gt1


from default_subreddit_meta dsm
left join default_sub_submissions dss on dsm.subreddit = dss.subreddit
where included = 'yes'
and (
		age(created_utc::date, date_trunc('month', dsm.added)) >= interval '-18 month'
		and 
		age(created_utc::date, date_trunc('month', dsm.added)) < interval '19 month'
	)
group by dss.subreddit, rel_month



---
---
---
---
---

with user_sub_activity_data as (
select 
	dsm.subreddit, 
	usds.author,
	sum(usds.total_items) as total_items,
	sum(usds.num_comments) as num_comments,
	sum(usds.num_submissions) as num_submissions
from default_subreddit_meta dsm
left join user_subreddit_daily_summary usds on usds.subreddit=dsm.subreddit
where age(usds.date::timestamp without time zone, date_trunc('month', dsm.added)) >= interval '-24 month'
	and age(usds.date::timestamp without time zone, date_trunc('month', dsm.added)) < interval '25 month'
	and usds.author != '[deleted]'
	and dsm.included = 'yes'
group by dsm.subreddit, usds.author
),
ranked_user_sub_data as (
	select 
		*,  
		rank() over (partition by subreddit order by total_items desc) as rank
	from user_sub_activity_data
),
top_users as (
select
	rusd.*
	from ranked_user_sub_data rusd
	where rusd.rank < 20
	order by rusd.subreddit, rusd.rank asc
)
select
 tu.*, usa.*
 from top_users tu
left join user_subreddit_activity usa on usa.subreddit=tu.subreddit and usa.author=tu.author



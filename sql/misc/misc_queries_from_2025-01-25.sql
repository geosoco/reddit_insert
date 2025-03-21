--
--
-- queries before shutdown 1/25/2025
--
--


with top_joining_articles as (
select 
article, count(*) as cnt
from s3_user_subreddit_first_activity_with_post
group by article
order by cnt desc
),
top_adv_posts as (
select
	target_id, count(*) as cnt
from s3_inbound_links_and_crossposts_for_graph
group by target_id
order by cnt desc
)
select
coalesce(tja.article, tap.target_id) as submission_id,
coalesce(tja.cnt, 0) as total_joins,
coalesce(tap.cnt, 0) as total_links,
s.created_utc, s.subreddit, s.data->>'title'
from top_joining_articles tja
full join top_adv_posts tap on tap.target_id = tja.article
left join s15_political_submissions s on s.id = coalesce(tja.article, tap.target_id)
order by total_joins desc





with posts as (
select
	target_id, count(*) as cnt
from s3_inbound_links_and_crossposts_for_graph
group by target_id
order by cnt desc
), top_posts as (
select *, rank() over (order by cnt desc)  as rank
from posts 
)
select
tp.*, s.created_utc, subreddit, s.author, s.data->>'title', s.data->>'score', s.data->>'num_comments'
from top_posts tp
left join s15_political_submissions s on s.id = tp.target_id
where rank < 1000
order by rank asc




with posts as (
select
	target_id, count(*) as cnt
from s3_inbound_links_and_crossposts_for_graph
group by target_id
order by cnt desc
), top_posts as (
select *, rank() over (order by cnt desc)  as rank
from posts 
)
select
tp.*, s.created_utc, subreddit, s.author, s.data->>'title', s.data->>'score', s.data->>'num_comments'
from top_posts tp
left join s15_political_submissions s on s.id = tp.target_id
where rank < 1000
order by rank asc








with top_submissions as (
	select id, author, is_text_post, score, num_comments, domain from s3_political_submissions where subreddit = 'The_Donald' order by score desc limit 10000
)
select
	domain, count(*) as cnt
from top_submissions
group by domain
order by cnt desc





with top_subs as (
select
*
from subreddit_summary ss
where total_activity > 500000
order by total_activity desc
)
select ts.name, total_activity, total_comments, total_submissions, unique_authors, s.created_utc 
from top_subs ts
left join subreddits s on ts.name = s.display_name 

where s.created_utc > '2010-12-31'
order by total_activity desc






with ranked_subs as (
	select mentioned_sub_name, 
	(total_comment_adv_comments + total_submission_adv_submissions) as overall_total_adv_content
	from subreddit_inbound_advertising_data
	order by (total_comment_adv_comments + total_submission_adv_submissions) desc nulls last
	limit 100
)
select rs.*, s.created_utc 
from ranked_subs rs
left join subreddits s on rs.mentioned_sub_name = s.display_name 
where s.created_utc > '2015-01-01'








with promos as (
	select
		case when comment_id is not null then 'c' 
		when submission_id is not null then 's' 
		else 'x' end as source_type
	from s3_inbound_adv_content_combined imcf
	where mentioned_sub_name = 'The_Donald'
	and author not in ('[deleted]', 'AutoModerator', 'BitcoinAllBot')
)
select
	source_type, count(*)
from promos
group by source_type









with bitcoin_comments as (
select
		(data->>'score')::int as score,
		id,
		article,
		created_utc,
		data->>'body'
	
	from comments_y2016_m10 c
	where subreddit = 'PoliticsAll'
	and author = 'BitcoinAllBot' 
	and created_utc > '2016-10-14'
	limit 2000
)
select bc.*, 
(s.data->>'score')::int as submission_score,
(s.data->>'num_comments')::int as submission_comments,
s.data->>'title',
s.author,
s.created_utc
from bitcoin_comments bc
left join submissions_y2016_m10 s on s.id = bc.article






with author_joins as (
	select 
		subreddit,
		author,
		first_activity_time,
		date_trunc('day', first_activity_time) as join_date,
		case when first_fostering_month is not null then 'foster'
		when num_days >= 1 then 'multiday'
		else 'day' end as duration			

	from s3_user_joins_with_fostering uj
	where subreddit = 'The_Donald'
),
promos as (
	select
		author,
		mentioned_sub_name,
		created_utc
	from s3_inbound_adv_content_combined imcf
	where mentioned_sub_name = 'The_Donald'
	and author not in ('[deleted]', 'AutoModerator')
),
combined as (
select
	p.author,
	p.created_utc,
	aj.author,
	aj.first_activity_time,
	duration,
	case when aj.author is null or aj.first_activity_time is null then 'never'
	when aj.first_activity_time >= p.created_utc then 'after'
	when aj.first_activity_time < p.created_utc then'before'
	end as rel_time
from promos p
left join author_joins aj on aj.subreddit = p.mentioned_sub_name and aj.author = p.author
)
select
duration, rel_time, count(*) as cnt
from combined
group by duration, rel_time;




with author_joins as (
	select 
		subreddit,
		author,
		first_activity_time,
		date_trunc('day', first_activity_time) as join_date,
		case when first_fostering_month is not null then 'foster'
		when num_days >= 1 then 'multiday'
		else 'day' end as duration			

	from s3_user_joins_with_fostering uj
	where subreddit = 'The_Donald'
),
promos as (
	select
		author,
		mentioned_sub_name,
		created_utc
	from s3_inbound_adv_content_combined imcf
	where mentioned_sub_name = 'The_Donald'
	and author not in ('[deleted]', 'AutoModerator', 'BitcoinAllBot')
),
combined as (
select
	p.author,
	p.created_utc,
	aj.author,
	aj.first_activity_time,
	duration,
	case when aj.author is null or aj.first_activity_time is null then 'never'
	when aj.first_activity_time >= p.created_utc then 'after'
	when aj.first_activity_time < p.created_utc then'before'
	end as rel_time
from promos p
left join author_joins aj on aj.subreddit = p.mentioned_sub_name and aj.author = p.author
)
select
duration, rel_time, count(*) as cnt
from combined
where rel_time = 'before'
group by duration, rel_time;

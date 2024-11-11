-----------------------------------------------
--
-- Some code generation queries
--
-- These are some misc queries used to generate coding sheets prior to the chicago trip that
-- may not be used in the final paper
--
-----------------------------------------------





-----------------------------------------------
--
--
--
-----------------------------------------------



select setseed(-0.314);

with 
segmented_text_posts as (
	select 
		case 
		when created_utc < '2015-07-01' then -1
		when created_utc >= '2015-07-01' and created_utc < '2016-02-01' then 0
		when created_utc >= '2016-02-01' and created_utc < '2016-08-01' then 1
		else 2 end as period,
		*
	from s3_political_submissions
	where has_removed_text = false	and is_text_post = true
),
random_text_posts as (
	select 
		row_number() over( partition by subreddit, period order by random()) as rownum,
		*
	from segmented_text_posts
	where period >= 0
)
select 
	rownum,
	subreddit,
	id,
	id36,
	created_utc,
	author, 
	title,
	selftext,
	'' as code1,
	'' as code2,
	'' as code3,
	'' as code4,
	'https://redd.it/' || id36,
	score,
	num_comments_from_data
	
from random_text_posts where rownum <= 400;










-----------------------------------------------
--
--
--
-----------------------------------------------



select setseed(0.867);

with year_subs as (
	select 
		display_name,
		created_utc, 
		ceil(extract(epoch from ('2017-01-01 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_days
	
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
),
sub_days as (
	select 
	ys.display_name as subreddit, 
	created_utc,
	max_days,
	day as creation_delta_days

	from year_subs ys
	cross join lateral generate_series(0, floor(ys.max_days)::int) m(day)
	order by ys.display_name, day
),
submission_links as (
select
	a.id as submission_id,
	a.mentioned_sub_name,
	a.subreddit, 
	a.created_utc,
	(extract(epoch from (a.created_utc - ys.created_utc))::bigint) / (3600*24)::int as creation_delta_days,
	a.author,
	a.link_type,
	a.mentioned_sub_link,
	a.source,
	a.sub_source
	
	from year_subs ys
	left join ( 
		select
			stsld.id, ys.display_name as mentioned_sub_name, stsld.subreddit, stsld.created_utc, 
			stsld.author, stsld.link_type, stsld.mentioned_sub_link, 
			's' as source, 'title' as sub_source
			
		from year_subs ys 
		left join s2_submission_title_sub_link_details stsld on lower(ys.display_name) = stsld.mentioned_sub_name
		where stsld.self_reference != TRUE 
		union all
		select
			distinct sssld.id, ys.display_name as mentioned_sub_name, sssld.subreddit, sssld.created_utc,
			sssld.author, sssld.link_type, sssld.mentioned_sub_link, 's' as source, 'selftext' as sub_source
		from year_subs ys 
		left join s2_submission_selftext_sub_link_details sssld on lower(ys.display_name) = sssld.mentioned_sub_name
		where sssld.self_reference != TRUE 
	) a	on a.mentioned_sub_name = ys.display_name
	--left join s2_submission_title_sub_link_details stsld on stsld.id = a.id
	--left join s2_submission_selftext_sub_link_details sssld on sssld.id = a.id
),
period_data as (
	select 
		case 
		when sl.created_utc < '2015-07-01' then -1
		when sl.created_utc >= '2015-07-01' and sl.created_utc < '2016-02-01' then 0
		when sl.created_utc >= '2016-02-01' and sl.created_utc < '2016-08-01' then 1
		else 2 end as period,
	sl.submission_id,
	sl.mentioned_sub_name,
	sl.subreddit,
	sl.created_utc,
	sl.author
from submission_links sl
where subreddit != 'autotldr'

),
random_text_posts as (
	select 
		row_number() over( partition by mentioned_sub_name, period order by random()) as rownum,
		*
	from period_data pd
	where period >= 0
),
limited_posts as (
	select * from random_text_posts rtp	
	where rownum <= 500
)
select
	lp.*,
	s.data->>'title' as title,
	s.data->>'selftext' as selftext,
	'' as code1,
	'' as code2,
	'' as code3,
	'' as code4,		
	s.data->>'url' as url,
	s.data->>'score' as score,
	s.data->>'num_comments' as num_comments
from limited_posts lp
left join submissions s on s.id = lp.submission_id

order by mentioned_sub_name, period asc;













-----------------------------------------------
--
--
--
-----------------------------------------------










select setseed(0.867);

with year_subs as (
	select 
		display_name,
		created_utc, 
		ceil(extract(epoch from ('2017-01-01 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_days
	
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
),
sub_days as (
	select 
	ys.display_name as subreddit, 
	created_utc,
	max_days,
	day as creation_delta_days

	from year_subs ys
	cross join lateral generate_series(0, floor(ys.max_days)::int) m(day)
	order by ys.display_name, day
),
comment_ads as (
	select
		sld.id as comment_id,
		ys.display_name as mentioned_sub_name,
		sld.subreddit,
		sld.created_utc,
		(extract(epoch from (sld.created_utc - ys.created_utc))::bigint) / (3600*24)::int as creation_delta_days,
		author,
		mentioned_sub_link
	
	from year_subs ys
	left join s2_comment_sub_link_details sld on sld.mentioned_sub_name = lower(ys.display_name)
	where self_reference != TRUE 
),
period_data as (
	select 
		case 
		when ca.created_utc < '2015-07-01' then -1
		when ca.created_utc >= '2015-07-01' and ca.created_utc < '2016-02-01' then 0
		when ca.created_utc >= '2016-02-01' and ca.created_utc < '2016-08-01' then 1
		else 2 end as period,
	ca.comment_id,
	ca.mentioned_sub_name,
	ca.subreddit,
	ca.created_utc,
	ca.author,
	c.data->>'body' as body,
	c.data->>'score' as score

from comment_ads ca
left join comments c on c.id = ca.comment_id
where c.data->>'body' != '[deleted]'
),
random_text_posts as (
	select 
		row_number() over( partition by mentioned_sub_name, period order by random()) as rownum,
		*
	from period_data
	where period >= 0
),
limited_posts as (
	select * from random_text_posts rtp	
	where rownum <= 300
)
select
	lp.*,
	'' as code1,
	'' as code2,
	'' as code3,
	'' as code4		
from limited_posts lp

order by mentioned_sub_name, period asc;













-----------------------------------------------
--
--
--
-----------------------------------------------

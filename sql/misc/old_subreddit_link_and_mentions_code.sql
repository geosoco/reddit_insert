-- here's some stuff about mentions
-- the best regex i've found is this:
-- regexp_matches(title, '(?:^|\s)/?r/(\w+)', 'gi')



-- not sure if these next few work, they're also not the best test. see the updated regex

select id, created_utc, count(regexp_matches(data->>'selftext', '/?(r/[a-z0-9_]+)', 'gi')) as target
from submissions
where id in (38496556,);


select id, created_utc, regexp_matches(data->>'selftext', '/?(r/[a-z0-9_]+)', 'gi') as target
from submissions
where id in (38496556,);


select created_utc, id, subreddit,  regexp_matches(s.data->>'body', '/?(r/[a-z0-9_]+)', 'gi') as target
from s1_coding_comments c
where (c.data->>'is_self')::boolean is true
and


--
-- this definitely works, and seems to work well
--

with title_mentioned_subs as (
select id, created_utc, subreddit, 
	unnest(regexp_matches(title, '(?:^|\s)/?r/(\w+)', 'gi')) as mentioned_sub,
	title
from coded_sub_submissions_details
where title ~* '(^|\s)/?r/\w+'
)
select * 
from title_mentioned_subs tms 
where lower(subreddit) != lower(mentioned_sub)
and not (title ~* '(x-post|xpost|crosspost|cross-post|repost|x/ post)'




with title_mentioned_subs as (
select id, created_utc, subreddit, unnest(regexp_matches(title, '(?:^|\s)/?r/(\w+)', 'gi')) as mentioned_sub, title
from coded_sub_submissions_details
where title ~* '(^|\s)/?r/\w+'
)
select id, count(*) as cnt 
from title_mentioned_subs
group by id
order by 
cnt desc



select id, created_utc, subreddit, regexp_matches(title, '(?:^|\s)/?r/(\w+)', 'gi') as mentioned_sub, title
from coded_sub_submissions_details
where created_utc >= '2012-09-01' and created_utc < '2012-09-10' and title ~* '(^|\s)/?r/\w+'




with comment_mentioned_subs as (
select id, 'https://redd.it/' || lower(base36_encode(id)), created_utc, author, subreddit, 
	unnest(regexp_matches(data->>'body', '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*', 'gi')) as mentioned_sub,
	data->>'body'
from s1_coding_comments
where data->>'body' ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
)
select * 
from comment_mentioned_subs cms 
where lower(subreddit) != lower(mentioned_sub)





with comment_mentioned_subs as (
select id, 'https://redd.it/' || lower(base36_encode(id)), created_utc, author, subreddit, 
	
	
	
		(select array_agg(mentioned_sub)  from (select distinct mentioned_sub from (
				select unnest(regexp_matches(data->>'body', '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*', 'gi')) as mentioned_sub
			) b1) ) as mentioned_sub,
	data->>'body'
from s1_coding_comments
where data->>'body' ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
	and id in (27359782383, 27004445716, 27100915819, 27256889208, 27459354274, 27733005786, 28033314331)
)
select * 
from comment_mentioned_subs cms 
where lower(subreddit) != lower(mentioned_sub)



select id, subreddit, distinct mentioned_sub
from (
	select 
		id, subreddit, 
		unnest(regexp_matches(data->>'body', '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*', 'gi')) as mentioned_sub 
	from s1_coding_comments
	where data->>'body' ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
		and id in (27359782383, 27004445716, 27100915819, 27256889208, 27459354274, 27733005786, 28033314331)

) b


with comment_mentioned_subs as (
	select id, 'https://redd.it/' || lower(base36_encode(id)) as url, created_utc, author, subreddit, 
	unnest(select distinct match from regexp_matches(data->>'body', '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*', 'gi') t(match))) as mentioned_sub,
	data->>'body' as body
from s1_coding_comments
where data->>'body' ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
)
select * 
from comment_mentioned_subs cms 
where lower(subreddit) != lower(mentioned_sub)




select id, created_utc, author, subreddit, 
	(select array_agg(i) from (
		select distinct (
			regexp_matches(
				data->>'body', 
				'(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*', 'gi'
			) 
		)[1] i
	) c) as mentioned_sub,
	data->>'body' as body
from s1_coding_comments
-- where id = 27359782383
where id in (27359782383, 27004445716, 27100915819, 27256889208, 27459354274, 27733005786, 28033314331)













with full_mentions as (
select
	id, created_utc, author, subreddit, 
	(select array_agg(y) from (select (regexp_matches(
				data->>'body', 
				'(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/(([\w\+\-]{2,}|reddit\.com)[\/\w\-]*)', 'gi'
			))[1] y) t )   as mentioned_sub_agg
from s1_coding_comments
-- where id = 27359782383
where data->>'body' ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
)
--, mentions_plus as (
	select 
		*, 
		array_length(mentioned_sub_agg, 1)  as total_mentions,
		(select array(select distinct lower(e) from unnest(mentioned_sub_agg) AS a(e)))
		from full_mentions
)
select
	id, created_utc, author, subreddit, total_mentions, (select distinct b from unnest(mentioned_sub_agg) as a(b))
	
	from mentions_plus
			



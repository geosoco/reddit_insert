--
--
-- Creates several large tables of links and mentions for the entire database
--
-- selftext - s2_submission_selftext_mentions_full
-- title - s2_submission_title_mentions_full
-- comments - 
--


--
-- selftext - s2_submission_selftext_mentions_full
--

drop table if exists s2_submission_selftext_mentions_full;

with full_mentions as (
select
	id, created_utc, author, subreddit, 
	(select array_agg(y) from (select (regexp_matches(
				data->>'selftext', 
				'(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/(([\w\+\-]{2,}|reddit\.com)[\/\w\-]*)', 'gi'
			))[1] y) t )   as mentioned_sub_link
from submissions
-- where id = 27359782383
where 
	data->>'is_self' = 'true' and
	data->>'selftext' != '' and
data->>'selftext' ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
)
select id, created_utc, author, subreddit, unnest(mentioned_sub_link) as mentioned_sub_link
into s2_submission_selftext_mentions_full
from full_mentions;



grant select on s2_submission_selftext_mentions_full to public;



--
-- title - s2_submission_title_mentions_full
--


drop table if exists s2_submission_title_mentions_full;

with full_mentions as (
select
	id, created_utc, author, subreddit, 
	(select array_agg(y) from (select (regexp_matches(
				data->>'title', 
				'(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/(([\w\+\-]{2,}|reddit\.com)[\/\w\-]*)', 'gi'
			))[1] y) t )   as mentioned_sub_link
from submissions
-- where id = 27359782383
where 
	data->>'is_self' = 'true' and
	data->>'title' != '' and
data->>'title' ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
)
select id, created_utc, author, subreddit, unnest(mentioned_sub_link) as mentioned_sub_link
into s2_submission_title_mentions_full
from full_mentions;


grant select on s2_submission_title_mentions_full to public;


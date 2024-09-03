--
-- create the comment mention/link intermediate table
--
-- the regex for detecting these is from reddit's source code from the update
--
-- the mentioned_sub
--

drop table if exists s1_coding_comment_subreddit_links_full;

with full_mentions as (
select
	id, created_utc, author, subreddit, 
	(select array_agg(y) from (select (regexp_matches(
				data->>'body', 
				'(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/(([\w\+\-]{2,}|reddit\.com)[\/\w\-]*)', 'gi'
			))[1] y) t )   as mentioned_sub_link
from s1_coding_comments
-- where id = 27359782383
where data->>'body' ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
)
select id, created_utc, author, subreddit, unnest(mentioned_sub_link) as mentioned_sub_link
into s1_coding_comment_subreddit_links_full
from full_mentions;



grant select on s1_coding_comment_subreddit_links_full to public;
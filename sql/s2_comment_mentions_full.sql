--
-- s2_comment_mentions_full
--	

drop table if exists s2_comment_mentions_full;

	with full_mentions as (
	select
		id, created_utc, author, subreddit, article, 
		(select array_agg(y) from (select (regexp_matches(
					data->>'body', 
					'(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/(([\w\+\-]{2,}|reddit\.com)[\/\w\-]*)', 'gi'
				))[1] y) t )   as mentioned_sub_link
	from comments
	-- where id = 27359782383
	where 
		data->>'body' ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
	)
	select id, created_utc, article, author, subreddit, unnest(mentioned_sub_link) as mentioned_sub_link
	into s2_comment_mentions_full
	from full_mentions;
	
	
	
	grant select on s2_comment_mentions_full to public;
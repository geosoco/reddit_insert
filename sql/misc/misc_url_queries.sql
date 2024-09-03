
with url_matches as
(
select
	(select * from regexp_matches(
				url, 
				'(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/(([\w\+\-]{2,}|reddit\.com)[\/\w\-]*)', 'gi'
			))[1] as url_matches,
	*
from coded_sub_submissions_details
where 
url ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
)
select 
	split_part(trim(both '/' from url_matches), '/', 1) as mentioned_sub,
	url_matches,
	* 
	from url_matches
	where strpos(trim(both '/' from url_matches), '/') < 1
	and length(split_part(trim(both '/' from url_matches), '/', 1)) < 32
	



select
	id, created_utc, subreddit, title, url, selftext
	from coded_sub_submissions_details
where domain ~* ('self\.|reddit.com')
and 
(url ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
 or
 title ~* '(?:\/|(?<=[!"\#\$%&''()*+,\-\.\/:;<=>\?@\[\\\]\^_`\{\|\}~\s])|(?<=\A))r\/([\w\+\-]{2,}|reddit\.com)[\/\w\-]*'
)



vacuum freeze s2_comment_sub_link_details;


select id, created_utc, author, article, subreddit, mentioned_sub_name, mentioned_sub_link from s2_comment_sub_link_details
where length(mentioned_sub_name) > 21;


select count(*) from s2_comment_sub_link_details;
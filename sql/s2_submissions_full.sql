--
--
--
--
--
--
--



drop table if exists s2_submissions_full;



with year_subs as (
	select display_name as subreddit, created_utc, subscribers
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' and display_name is not null
),
comment_counts as (
	select
		ys.subreddit, ser.id, ser.comment_count_from_data, ser.automod_comment_count
	from year_subs ys
	left join submission_engagement_raw ser on ser.subreddit = ys.subreddit
)


select
	coalesce(s.id, cc.id) as id, 
	s.data->>'id' as id36,
	s.created_utc,
	s.subreddit,
	author,

	(data->>'is_self')::bool as is_text_post,
	(data->>'selftext' is not null AND 
	 data->>'selftext' != '[deleted]' AND
	 data->>'selftext' != '[removed]' AND
	 length(data->>'selftext') > 0
	) as has_body_text,
	data->>'selftext' = '[deleted]' as has_deleted_text,
	data->>'selftext' = '[removed]' as has_removed_text,
 
	(data->>'score')::integer as score, 
	(data->>'num_comments')::integer as num_comments_from_json,
	coalesce(cc.comment_count_from_data,0) as num_comments_from_data,
	coalesce(cc.automod_comment_count,0) as num_automod_comments,
	
	data->>'title' as title,
	data->>'url' as url,
	data->>'domain' as domain,
	data->>'selftext' as selftext

	

into s2_submissions_full
from year_subs ys
left join submissions s on s.subreddit = ys.subreddit
left join comment_counts cc on cc.id = s.id
where s.created_utc >= '2012-01-01';




grant select on s2_submissions_full to public;

create index on s2_submissions_full(subreddit);
create index on s2_submissions_full(author);
create index on s2_submissions_full(id);
create index on s2_submissions_full(created_utc);
create index on s2_submissions_full(subreddit, author);
create index on s2_submissions_full(title);


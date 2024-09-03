
--select * from s3_political_submissions where 


drop table if exists s3_political_submissions;


with comment_counts as (
	select
		article, count(*) as count
		from comments 
		where subreddit in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
		and created_utc >= '2012-05-01'
		group by article
)

select
	coalesce(s.id, cc.article) as id, 
	s.data->>'id' as id36,
	created_utc,
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
	(data->>'num_comments')::integer as num_comments,
	coalesce(cc.count,0) as num_comments_from_data,
	
	data->>'title' as title,
	data->>'url' as url,
	data->>'domain' as domain,
	data->>'selftext' as selftext,
	data
into s3_political_submissions
from submissions s
full outer join comment_counts cc on cc.article = s.id	
where s.subreddit in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
and created_utc >= '2012-05-01';



grant select on s3_political_submissions to public;

create index on s3_political_submissions(subreddit);
create index on s3_political_submissions(author);
create index on s3_political_submissions(id);
create index on s3_political_submissions(created_utc);
create index on s3_political_submissions(subreddit, author);
create index on s3_political_submissions(title);


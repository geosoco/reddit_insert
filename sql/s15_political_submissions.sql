--
--
-- pulled from the query log
--
-- this seems to be the last one, but this shouldn't be used. It's been updated to be s3, and there's
-- a better query there
--
--

with comment_counts as (
		select article, count(*) as count
		from comments 
		where subreddit in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
		and created_utc >= '2012-05-01' 
		group by article
	)
	select
		id, data->>'id' as id36, created_utc, s.subreddit, author, 
			case when data->>'selftext' = '[deleted]' then '[deleted]' 
				when data->>'domain' = ('self.' || s.subreddit) then 'self'
				else 'link' end as submission_type,
		length(data->>'selftext') as self_length,
		data->>'domain' as domain,
		data->>'score' as score,
		data->>'num_comments' as num_comments,
		coalesce(cc.count, 0) as num_comments_from_data,
		s.data as data
		into s15_political_submissions
	    from submissions s
		full outer join comment_counts cc on cc.article = s.id
		where s.subreddit in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
		and created_utc >= '2012-05-01'
	
		order by subreddit, created_utc asc
	
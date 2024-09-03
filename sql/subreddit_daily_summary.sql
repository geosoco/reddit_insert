
DROP TABLE IF EXISTS subreddit_comments_summary_daily;

CREATE TABLE subreddit_comments_summary_daily
(
    id integer PRIMARY KEY,
    date date,
    subreddit text,
    total_count integer,
    deleted_count integer,
    removed_count integer,
    deleted_author_count integer,
    automod_count integer,
    unique_authors integer,
);


DROP TABLE IF EXISTS subreddit_submissions_summary_daily;

CREATE TABLE subreddit_submissions_summary_daily
(
    id integer PRIMARY KEY,
    date date,
    subreddit text,
    total_count integer,
    deleted_count integer,
    removed_count integer,
    deleted_author_count integer,
    automod_count integer,
    unique_authors integer,
);




INSERT INTO subreddit_comments_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors)  (
		select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'body' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'body' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from comments
			group by date_trunc('day', created_utc)::date, subreddit
	)



INSERT INTO subreddit_submissions_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors)  (
		select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'selftext' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'selftext' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from submissions
			group by date_trunc('day', created_utc)::date, subreddit
	)



/* 

t_comments_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) 
		(select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'body' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'body' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from comments
			where created_utc < '2013-01-01'
			group by date_trunc('day', created_utc)::date, subreddit
	);



INSERT INTO subreddit_comments_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) 
		(select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'body' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'body' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from comments
			where created_utc >= '2013-01-01' and created_utc < '2015-01-01'
			group by date_trunc('day', created_utc)::date, subreddit
	);



INSERT INTO subreddit_comments_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) 
		(select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'body' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'body' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from comments
			where created_utc >= '2015-01-01'
			group by date_trunc('day', created_utc)::date, subreddit
	);




INSERT INTO subreddit_submissions_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) 
		(select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'selftext' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'selftext' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from submissions
			where created_utc < '2009-01-01'
			group by date_trunc('day', created_utc)::date, subreddit
	);
              



INSERT INTO subreddit_submissions_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) 
		(select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'selftext' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'selftext' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from submissions
			where created_utc >= '2009-01-01' and created_utc < '2011-01-01'
			group by date_trunc('day', created_utc)::date, subreddit
	);
              



INSERT INTO subreddit_submissions_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) 
		(select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'selftext' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'selftext' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from submissions
			where created_utc >= '2011-01-01' and created_utc < '2013-01-01'
			group by date_trunc('day', created_utc)::date, subreddit
	);
      


INSERT INTO subreddit_submissions_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) 
		(select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'selftext' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'selftext' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from submissions
			where created_utc >= '2013-01-01' and created_utc < '2014-01-01'
			group by date_trunc('day', created_utc)::date, subreddit
	);        



INSERT INTO subreddit_submissions_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) 
		(select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'selftext' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'selftext' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from submissions
			where created_utc >= '2014-01-01' and created_utc < '2015-01-01'
			group by date_trunc('day', created_utc)::date, subreddit
	);        




INSERT INTO subreddit_submissions_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) 
		(select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'selftext' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'selftext' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from submissions
			where created_utc >= '2015-01-01' and created_utc < '2016-01-01'
			group by date_trunc('day', created_utc)::date, subreddit
	);        






INSERT INTO subreddit_submissions_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) 
		(select date_trunc('day', created_utc)::date as date,
			subreddit,
			count(*) as total_count,
			count(*) FILTER (where data->>'selftext' = '[deleted]') as deleted_count,
			count(*) FILTER (where data->>'selftext' = '[removed]') as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors
			from submissions
			where created_utc >= '2016-01-01'
			group by date_trunc('day', created_utc)::date, subreddit
	);        

*/
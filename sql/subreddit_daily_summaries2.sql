
                  
                  
DROP TABLE IF EXISTS subreddit_comments_summary_daily;

CREATE TABLE subreddit_comments_summary_daily
(
    id serial PRIMARY KEY,
    date date,
    subreddit text,
    total_count integer,
    deleted_count integer,
    removed_count integer,
    deleted_author_count integer,
    automod_count integer,
    unique_authors integer
);


DROP TABLE IF EXISTS subreddit_submissions_summary_daily;

CREATE TABLE subreddit_submissions_summary_daily
(
    id serial PRIMARY KEY,
    date date,
    subreddit text,
    total_count integer,
    deleted_count integer,
    removed_count integer,
    deleted_author_count integer,
    automod_count integer,
    unique_authors integer
);




INSERT INTO subreddit_comments_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) values (
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
	);



INSERT INTO subreddit_submissions_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors) values (
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
	);
              




DROP TABLE IF EXISTS subreddit_summary_daily;

CREATE TABLE subreddit_summary_daily
(
    id serial PRIMARY KEY,
    date date,
    subreddit text,

	total_count integer,
    deleted_count integer,
    removed_count integer,
    deleted_author_count integer,
    automod_count integer,
    unique_authors integer default null,
	
    comments_total_count integer,
    comments_deleted_count integer,
    comments_removed_count integer,
    comments_deleted_author_count integer,
    comments_automod_count integer,
    comments_unique_authors integer,

    submissions_total_count integer,
    submissions_deleted_count integer,
    submissions_removed_count integer,
    submissions_deleted_author_count integer,
    submissions_automod_count integer,
    submissions_unique_authors integer

);




INSERT INTO subreddit_summary_daily (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count,

	comments_total_count, comments_deleted_count, comments_removed_count, comments_deleted_author_count, comments_automod_count, comments_unique_authors,

	submissions_total_count, submissions_deleted_count, submissions_removed_count, submissions_deleted_author_count, submissions_automod_count, submissions_unique_authors	
	)  (
		select 
			    coalesce(ss.date, cs.date),
			    coalesce(ss.subreddit, cs.subreddit),

			    coalesce(ss.total_count,0) + coalesce(cs.total_count, 0),
			    coalesce(ss.deleted_count,0) + coalesce(cs.deleted_count, 0),
			    coalesce(ss.removed_count,0) + coalesce(cs.removed_count, 0),
			    coalesce(ss.deleted_author_count, 0) + coalesce(cs.deleted_author_count, 0),
			    coalesce(ss.automod_count,0) + coalesce(cs.automod_count, 0),

			    coalesce(cs.total_count, 0),
			    coalesce(cs.deleted_count, 0),
			    coalesce(cs.removed_count, 0),
			    coalesce(cs.deleted_author_count, 0),
			    coalesce(cs.automod_count, 0),
			    coalesce(cs.unique_authors, 0),

			    coalesce(ss.total_count, 0),
			    coalesce(ss.deleted_count, 0),
			    coalesce(ss.removed_count, 0),
			    coalesce(ss.deleted_author_count, 0),
			    coalesce(ss.automod_count, 0),
			    coalesce(ss.unique_authors, 0)


			from  subreddit_submissions_summary_daily ss
			full outer join subreddit_comments_summary_daily cs on (ss.date = cs.date and ss.subreddit = cs.subreddit)


	);










create temp table subreddit_unique_users (
date date, 
subreddit text,
count bigint); 


insert into subreddit_unique_users (date, subreddit, count)
(select date, subreddit, count(*) as unique_authors from 
(select created_utc::date as date, subreddit, author from comments
union
select created_utc::date as date, subreddit, author from submissions
) a 
group by date, subreddit);



insert into subreddit_unique_users (date, subreddit, count)
(select date, subreddit, count(*) as unique_authors from 
(select distinct created_utc::date as date, subreddit, author from comments
union
select distinct created_utc::date as date, subreddit, author from submissions
) a 
group by date, subreddit);



update subreddit_summary_daily ssd
set unique_authors = a.count from 
(select date, subreddit, count from subreddit_unique_users) a
where ssd.date = a.date and ssd.subreddit = a.subreddit;




update subreddit_summary_daily ssd
set unique_authors = a.unique_authors from
(select date, subreddit, count(*) as unique_authors from 
(select created_utc::date as date, subreddit, author from comments
union
select created_utc::date as date, subreddit, author from submissions
) a 
group by date, subreddit) a
where ssd.date = a.date and ssd.subreddit = a.subreddit;








select date, subreddit, count(*) as unique_authors from 
(select distinct created_utc::date as date, distinct subreddit, distinct author from comments
union
select distinct created_utc::date as date, distinct subreddit, distinct author from submissions
) a 
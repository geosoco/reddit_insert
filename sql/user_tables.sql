/*
 *
 * User Comments
 *
 */


DROP TABLE IF EXISTS user_comments;

CREATE TABLE user_comments
(
    id bigint primary key,
    created_utc timestamp without time zone,
    author text,    
    subreddit text,
    score int,
    length int,
    deleted bool,
    removed bool
) tablespace ddrive;


INSERT INTO user_comments select id, created_utc, author, subreddit,
(data->>'score')::int,
char_length(data->>'body'),
data->>'body' = '[deleted]',
data->>'body' = '[removed]'
from comments;


create index on user_comments(created_utc);

cluster user_comments using user_comments_created_utc_idx ;

--alter table user_comments set logged;


create index on user_comments(author);
create index on user_comments(subreddit);
create index on user_comments(author, created_utc);
create index on user_comments(subreddit, created_utc);

analyze user_comments;


--
 --
 -- User Submissions
 --
 --

DROP TABLE IF EXISTS user_submissions;

CREATE TABLE user_submissions
(
    id bigint primary key,
    created_utc timestamp without time zone,
    author text,    
    subreddit text,
    score int,
    length int,
    deleted bool,
    removed bool,
    url text,
    domain text
) tablespace ddrive;


INSERT INTO user_submissions select id, created_utc, author, subreddit,
(data->>'score')::int,
char_length(data->>'selftext'),
data->>'selftext' = '[deleted]',
data->>'selftext' = '[removed]',
data->>'url',
substring(data->>'url' from '(?:.*://)?(?:www\.)?([^/]*)')
from submissions;


create index on user_submissions(created_utc);

cluster user_submissions using user_submissions_created_utc_idx ;

-- alter table user_submissions set logged;


create index on user_submissions(author);
create index on user_submissions(subreddit);
create index on user_submissions(author, created_utc);
create index on user_submissions(subreddit, created_utc);

analyze user_submissions;











create table user_subreddit_comments
(
	id serial primary key,
	date date,
	author text,
	subreddit text,
	total_posts integer,
	unique_subreddits integer
) tablespace ddrive;





DROP TABLE IF EXISTS user_comments_summary_daily;

CREATE TABLE user_comments_summary_daily
(
    id serial PRIMARY KEY,
    date date,
    author text,    
    total_count integer,
    unique_subreddits integer,
    timestamp min_time,
    timestamp max_time
) tablespace ddrive;


DROP TABLE IF EXISTS user_submissions_summary_daily;

CREATE TABLE user_submissions_summary_daily
(
    id serial PRIMARY KEY,
    date date,
    author text,
    total_count integer,
    deleted_count integer,
    removed_count integer,
    deleted_author_count integer,
    automod_count integer,
    unique_authors integer
) tablespace ddrive;

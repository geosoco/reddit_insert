DROP TABLE IF EXISTS subreddit_summary_monthly_defaults;

CREATE TABLE subreddit_summary_monthly_defaults
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

grant select on subreddit_summary_monthly_defaults to public;


INSERT INTO subreddit_summary_monthly_defaults (
	date,subreddit, total_count, deleted_count, removed_count, deleted_author_count, automod_count, unique_authors, 

	comments_total_count, comments_deleted_count, comments_removed_count, comments_deleted_author_count, comments_automod_count, comments_unique_authors,

	submissions_total_count, submissions_deleted_count, submissions_removed_count, submissions_deleted_author_count, submissions_automod_count, submissions_unique_authors	
	)  (
select
			date_trunc('month', created_utc) as date, 
			dsm.subreddit,
			count(*) as total_count,
			count(*) FILTER (where deleted = TRUE) as deleted_count,
			count(*) FILTER (where uca.removed = TRUE) as removed_count,
			count(*) FILTER (where author = '[deleted]') as deleted_author_count,
			count(*) FILTER (where author = 'AutoModerator') as automod_count,
			count(distinct author) as unique_authors,

			count(*) filter (where c_id is not null) as comments_total_count,
			count(*) FILTER (where c_id is not null and deleted = TRUE) as comments_deleted_count,
			count(*) FILTER (where c_id is not null and uca.removed = TRUE) as comments_removed_count,
			count(*) FILTER (where c_id is not null and author = '[deleted]') as comments_deleted_author_count,
			count(*) FILTER (where c_id is not null and author = 'AutoModerator') as comments_automod_count,
			count(distinct author) filter (where c_id is not null) as comments_unique_authors,
			
			count(*) filter (where s_id is not null) as submissions_total_count,
			count(*) FILTER (where s_id is not null and deleted = TRUE) as submissions_deleted_count,
			count(*) FILTER (where s_id is not null and uca.removed = TRUE) as submissions_removed_count,
			count(*) FILTER (where s_id is not null and author = '[deleted]') as submissions_deleted_author_count,
			count(*) FILTER (where s_id is not null and author = 'AutoModerator') as submissions_automod_count,
			count(distinct author) filter (where s_id is not null) as submissions_unique_authors		

			
			
		from default_subreddit_meta dsm
		left join user_combined_activity uca on uca.subreddit = dsm.subreddit
		where dsm.included = TRUE  
		and created_utc >= '2012-01-01 00:00:00'	
		group by dsm.subreddit, date
		order by date, dsm.subreddit
		

	);

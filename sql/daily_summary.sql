--
--
--
-- daily summary 
--
--
--
--

drop table if exists daily_activity_summary;

create table daily_activity_summary(
    id 						bigserial,
    date 					date,
    total_activity 			bigint,
    unique_authors			int,
    unique_subreddits		int,

    submissions				int,
    comments				int,
    deleted					int,
    removed					int,
    submissions_deleted		int,
	submissions_removed		int,
	comments_deleted		int,
	comments_removed		int,

	sum_score				bigint,

    total_score_negative_items bigint,
    total_score_zero_items bigint,
    total_score_one_items bigint,
    total_score_positive_gt1_items bigint
);



insert into daily_activity_summary (
	date, total_activity, unique_authors, unique_subreddits,
	submissions, comments, deleted, removed,
	submissions_deleted, submissions_removed, comments_deleted, comments_removed,
	sum_score,
	total_score_negative_items,
	total_score_zero_items,
	total_score_one_items,
	total_score_positive_gt1_items
) (
select 
	date_trunc('day', created_utc) as date,
	count(*) as total_activity,
	count(distinct author) as unique_authors,
	count(distinct subreddit) as unique_subreddits,
	count(*) filter(where s_id is not null) as submissions,
	count(*) filter(where c_id is not null) as comments,
	count(*) filter(where deleted is true) as deleted,
	count(*) filter(where removed is true) as removed,
	count(*) filter(where deleted is true and s_id is not null) as submissions_deleted,
	count(*) filter(where removed is true and s_id is not null) as submissions_removed,
	count(*) filter(where deleted is true and c_id is not null) as comments_deleted,
	count(*) filter(where removed is true and c_id is not null) as comments_removed,
	sum(score) as sum_score,
    count(*) filter(where score < 0) as total_score_negative_items,
    count(*) filter(where score = 0) as total_score_zero_items,
    count(*) filter(where score = 1) as total_score_one_items,
    count(*) filter(where score > 1) as total_score_positive_gt1_items

from user_combined_activity
group by date_trunc('day', created_utc)
order by date asc);
	


--
--
--
--
--

--
--
--
--
-- USER SUMMARY
--
--
--
--

drop table if exists user_summary;

create table user_summary(
    id bigserial,
    author text,

    total_subreddits bigint,
    total_comments bigint,
    total_submissions bigint,
    total_activity bigint,

    total_active_days bigint,

    total_deleted bigint,
    total_deleted_comments bigint,
    total_deleted_submissions bigint,
    total_removed bigint,
    total_removed_comments bigint,
    total_removed_submissions bigint,

    first_activity_time timestamp without time zone,
    last_activity_time timestamp without time zone,
    first_comment_time timestamp without time zone,
    last_comment_time timestamp without time zone,
    first_submission_time  timestamp without time zone,
    last_submission_time timestamp without time zone,
    first_comment_id bigint,
    last_comment_id bigint,
    first_submission_id bigint,
    last_submission_id bigint,

    first_activity_subreddit text,
    last_activity_subreddit text,
    first_comment_subreddit text,
    last_comment_subreddit text,
    first_submission_subreddit text,
    last_submission_subreddit text,

    comment_score_min int,
    comment_score_max int,
    comment_score_sum bigint,
    comment_score_avg numeric(20,4),

    submission_score_min int,
    submission_score_max int,
    submission_score_sum bigint,
    submission_score_avg numeric(20,4),

    combined_score_min int,
    combined_score_max int,
    combined_score_sum bigint,
    combined_score_avg numeric(20,4),

    comment_length_min int,
    comment_length_max int,
    comment_length_sum bigint,
    comment_length_avg numeric(20,4),

    submission_length_min int,
    submission_length_max int,
    submission_length_sum bigint,
    submission_length_avg numeric(20,4),

    combined_length_min int,
    combined_length_max int,
    combined_length_sum bigint,
    combined_length_avg numeric(20,4),


    total_score_negative_items bigint,
    total_score_zero_items bigint,
    total_score_positive_items bigint,

    num_sessions bigint,

    subreddits_per_session_min  int,
    subreddits_per_session_max  int,
    subreddits_per_session_avg  numeric(20,4),

    min_activity_in_session   int,
    max_activity_in_session   int,
    avg_activity_in_session   numeric(20,4),

    min_delta_time_in_session   int,
    max_delta_time_in_session   int,
    avg_delta_time_in_session   numeric(20,4)
);




insert into user_summary(
author, 
total_subreddits, total_comments, total_submissions, total_activity,
total_active_days,
total_deleted, total_deleted_comments, total_deleted_submissions,
total_removed, total_removed_comments, total_removed_submissions,
first_activity_time, last_activity_time, first_comment_time, last_comment_time,
first_submission_time, last_submission_time,
first_comment_id, last_comment_id,
first_submission_id, last_submission_id,
first_activity_subreddit, last_activity_subreddit,
first_comment_subreddit, last_comment_subreddit,
first_submission_subreddit, last_submission_subreddit,
comment_score_min, comment_score_max, comment_score_sum, comment_score_avg,
submission_score_min, submission_score_max, submission_score_sum, submission_score_avg,
combined_score_min, combined_score_max, combined_score_sum, combined_score_avg,
comment_length_min, comment_length_max, comment_length_sum, comment_length_avg,
submission_length_min, submission_length_max, submission_length_sum, submission_length_avg,
combined_length_min, combined_length_max, combined_length_sum, combined_length_avg,
total_score_negative_items, total_score_zero_items, total_score_positive_items,
num_sessions,
subreddits_per_session_min, subreddits_per_session_max, subreddits_per_session_avg,
min_activity_in_session, max_activity_in_session, avg_activity_in_session,
min_delta_time_in_session, max_delta_time_in_session, avg_delta_time_in_session
)
(
    select a.author,
        a.total_subreddits,
        a.total_comments,
        a.total_submissions,
        a.total_activity,

        a.total_active_days,

        a.total_deleted,
        a.total_deleted_comments,
        a.total_deleted_submissions,
        a.total_removed,
        a.total_removed_comments,
        a.total_removed_submissions,

        a.first_activity_time as first_activity_time,
        a.last_activity_time as last_activity_time,
        a.first_comment_time as first_comment_time,
        a.last_comment_time as last_comment_time,
        a.first_submission_time as first_submission_time,
        a.last_submission_time as last_submission_time,

        c.first_comment_id as first_comment_id,
        c.last_comment_id as last_comment_id,
        s.first_submission_id as first_submission_id,
        s.last_submission_id as last_submission_id,

        case when s.first_submission_time < c.first_comment_time then s.first_submission_subreddit
        else c.first_comment_subreddit end as first_activity_subreddit,
        case when s.last_submission_time > c.last_comment_time then s.last_submission_subreddit
        else c.last_comment_subreddit end as last_activity_subreddit,
        c.first_comment_subreddit,
        c.last_comment_subreddit,
        s.first_submission_subreddit,
        s.last_submission_subreddit,


        a.comment_score_min,
        a.comment_score_max,
        a.comment_score_sum,
        a.comment_score_avg,

        a.submission_score_min,
        a.submission_score_max,
        a.submission_score_sum,
        a.submission_score_avg,

        a.combined_score_min,
        a.combined_score_max,
        a.combined_score_sum,
        a.combined_score_avg, 



        a.comment_length_min,
        a.comment_length_max,
        a.comment_length_sum,
        a.comment_length_avg,

        a.submission_length_min,
        a.submission_length_max,
        a.submission_length_sum,
        a.submission_length_avg,

        a.combined_length_min,
        a.combined_length_max,
        a.combined_length_sum,
        a.combined_length_avg, 


        a.total_score_negative_items,
        a.total_score_zero_items,
        a.total_score_positive_items,

        a.num_sessions,


        a.subreddits_per_session_min,
        a.subreddits_per_session_max,
        a.subreddits_per_session_avg,

        a.min_activity_in_session,
        a.max_activity_in_session,
        a.avg_activity_in_session,

        a.min_delta_time_in_session,
        a.max_delta_time_in_session,
        a.avg_delta_time_in_session


        from user_combined_activity_summary a
        left join user_comments_summary c on c.author = a.author
        left join user_submissions_summary s on s.author = a.author
);
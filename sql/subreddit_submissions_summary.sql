
DROP TABLE IF EXISTS subreddit_submissions_summary;

CREATE TABLE subreddit_submissions_summary
(
    id serial PRIMARY KEY,
    created_utc timestamp without timezone,
    author text,
    subreddit text,

    is_removed boolean,
    is_deleted boolean,

    score int,
    comment_sum_score int,
    comment_mean_score numeric(20,4),

    num_comments int,
    found_comments int,
    deleted_comments int,
    deleted_author_comments int,
    removed_comments int,

    unique_authors int,
    count_root_comments int,

    total_score_negative_comments integer,
    total_score_zero_comments integer,
    total_score_one_comments integer,
    total_score_positive_gt1_comments integer

)
--
--
--
-- user submissions_summary
--
--
--
--

drop table if exists user_submissions_summary;

create table user_submissions_summary(
    id bigserial,
    author  text,

    total_submissions bigint,

    first_submission_time timestamp without time zone,
    last_submission_time timestamp without time zone,
    first_submission_id bigint,
    last_submission_id bigint,

    first_submission_subreddit text,
    last_submission_subreddit text
);


insert into user_submissions_summary(
author, 
total_submissions,
first_submission_time, last_submission_time,
first_submission_id, last_submission_id,
first_submission_subreddit, last_submission_subreddit
)
(
    select distinct on (author)
        author,
        count(*) over w as total_submissions,
        first_value(created_utc) over w as first_submission_time,
        last_value(created_utc) over w as last_submission_time,
        first_value(id) over w as first_submission_id,
        last_value(id) over w as last_submission_id,
        first_value(subreddit) over w as first_submission_subreddit,
        last_value(subreddit) over w as last_submission_subreddit
        from user_submissions
        window w as (partition by author order by created_utc, id asc range between unbounded preceding and unbounded following)

);


\copy (select distinct on (author) author, count(*) over w as total_submissions, first_value(created_utc) over w as first_submission_time, last_value(created_utc) over w as last_submission_time, first_value(id) over w as first_submission_id, last_value(id) over w as last_submission_id, first_value(subreddit) over w as first_submission_subreddit, last_value(subreddit) over w as last_submission_subreddit from user_submissions window w as (partition by author order by created_utc, id asc range between unbounded preceding and unbounded following)) to '/mnt/h/cygwin64/home/soco/submission_summary.csv' with csv delimiter ',';



DROP TABLE IF EXISTS user_subreddit_comments_summary;

CREATE TABLE user_subreddit_comments_summary
(
    id bigserial,
    author text,
    subreddit text,

    total_comments bigint,
    total_deleted bigint,
    total_removed bigint,

    first_comment_time timestamp without time zone,
    last_comment_time timestamp without time zone,

    first_comment_id bigint,
    last_comment_id bigint,

    comment_score_min int,
    comment_score_max int,
    comment_score_sum bigint,
    comment_score_avg int,

    comment_length_min int,
    comment_length_max int,
    comment_length_sum bigint,
    comment_length_avg int
);


insert into user_subreddit_comments_summary
(
    author, subreddit, 
    total_comments, total_deleted, total_removed, 
    first_comment_time, last_comment_time, 
    first_comment_id, last_comment_id, 
    comment_score_min, comment_score_max, comment_score_sum, comment_score_avg, 
    comment_length_min, comment_length_max, comment_length_sum, comment_length_avg        
) (
select 
    author, subreddit,
    count(*) as total_comments, sum(case when deleted is true then 1 else 0 end) as total_deleted,
    sum(case when removed is true then 1 else 0 end) as total_removed,
    min(created_utc) as first_comment_time, max(created_utc) as last_comment_time,
    min(first_comment_id) as first_comment_id, max(last_comment_id) as last_comment_id,
    min(score) as comment_score_min, max(score) as comment_score_max, 
    sum(score) as comment_score_sum, avg(score) as comment_score_avg,
    min(length) as comment_length_min, max(length) as comment_length_max, 
    sum(length) as comment_length_sum, avg(length) as comment_length_avg
    from (
        select author, subreddit, created_utc, score, length, deleted, removed, 
        first_value(id) over w as first_comment_id, last_value(id) over w as last_comment_id
        from user_comments
        window w as (partition by author, subreddit order by created_utc asc range between unbounded preceding and unbounded following)) a
    group by author, subreddit
    
);



DROP TABLE IF EXISTS user_subreddit_submissions_summary;

CREATE TABLE user_subreddit_submissions_summary
(
    id bigserial,
    author text,
    subreddit text,

    total_submissions bigint,
    total_submissions_deleted bigint,
    total_submissions_removed bigint,

    first_submission_time timestamp without time zone,
    last_submission_time timestamp without time zone,

    first_submission_id bigint,
    last_submission_id bigint,

    submission_score_min int,
    submission_score_max int,
    submission_score_sum bigint,
    submission_score_avg int,

    submission_length_min int,
    submission_length_max int,
    submission_length_sum bigint,
    submission_length_avg int
);


insert into user_subreddit_submissions_summary
(
    author, subreddit, 
    total_submissions, total_submissions_deleted, total_submissions_removed, 
    first_submission_time, last_submission_time, 
    first_submission_id, last_submission_id, 
    submission_score_min, submission_score_max,
    submission_score_sum, submission_score_avg, 
    submission_length_min, submission_length_max, 
    submission_length_sum, submission_length_avg        
) (
select 
    author, subreddit,
    count(*) as total_submissions, sum(case when deleted is true then 1 else 0 end) as total_submissions_deleted,
    sum(case when removed is true then 1 else 0 end) as total_submissions_removed,
    min(created_utc) as first_submission_time, max(created_utc) as last_submission_time,
    min(first_submission_id) as first_submission_id, max(last_submission_id) as last_submission_id,
    min(score) as submission_score_min, max(score) as submission_score_max, 
    sum(score) as submission_score_sum, avg(score) as submission_score_avg,
    min(length) as submission_length_min, max(length) as submission_length_max, 
    sum(length) as submission_length_sum, avg(length) as submission_length_avg
    from (
        select author, subreddit, created_utc, score, length, deleted, removed, 
        first_value(id) over w as first_submission_id, last_value(id) over w as last_submission_id
        from user_submissions
        window w as (partition by author, subreddit order by created_utc asc range between unbounded proceding and unbounded following)) a
    group by author, subreddit
    
);






create index on user_subreddit_comments_summary(author,subreddit);
create index on user_subreddit_submissions_summary(author,subreddit);






DROP TABLE IF EXISTS user_subreddit_activity;

CREATE TABLE user_subreddit_activity
(
    id bigserial,
    author text,
    subreddit text,

    total_comments bigint,
    total_submissions bigint,
    total_activity bigint,

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

    comment_score_min int,
    comment_score_max int,
    comment_score_sum bigint,
    comment_score_avg numeric(24,4),

    submission_score_min int,
    submission_score_max int,
    submission_score_sum bigint,
    submission_score_avg numeric(24,4),

    combined_score_min int,
    combined_score_max int,
    combined_score_sum bigint,
    combined_score_avg numeric(24,4),

    comment_length_min int,
    comment_length_max int,
    comment_length_sum bigint,
    comment_length_avg numeric(24,4),

    submission_length_min int,
    submission_length_max int,
    submission_length_sum bigint,
    submission_length_avg numeric(24,4),

    combined_length_min int,
    combined_length_max int,
    combined_length_sum bigint,
    combined_length_avg numeric(24,4),


    total_score_negative_items bigint,
    total_score_zero_items bigint,
    total_score_positive_items bigint,

    num_sessions bigint

);



insert into user_subreddit_activity
    (author, subreddit, 
    total_comments, total_submissions, total_activity,
    total_deleted, total_deleted_comments, total_deleted_submissions,
    total_removed, total_removed_comments, total_removed_submissions,
    first_activity_time, last_activity_time, 
    first_comment_time, last_comment_time,
    first_submission_time, last_submission_time,
    first_comment_id, last_comment_id, 
    first_submission_id, last_submission_id, 
    comment_score_min, comment_score_max, 
    comment_score_sum, comment_score_avg, 
    submission_score_min, submission_score_max, 
    submission_score_sum, submission_score_avg, 
    combined_score_min, combined_score_max, combined_score_sum, combined_score_avg,
    comment_length_min, comment_length_max, 
    comment_length_sum, comment_length_avg, 
    submission_length_min, submission_length_max, 
    submission_length_sum, submission_length_avg,
    combined_length_min, combined_length_max, combined_length_sum, combined_length_avg, 
    total_score_negative_items, total_score_zero_items, total_score_positive_items,
    num_sessions)
(
    select
        a.author, a.subreddit,
        c.total_comments as total_comments, 
        s.total_submissions as total_submissions,
        a.total_activity as total_activity,

        a.total_deleted as total_deleted,
        c.total_deleted as total_deleted_comments,
        s.total_submissions_deleted as total_deleted_submissions,

        a.total_removed as total_removed,
        c.total_removed as total_removed_comments,
        s.total_submissions_removed as total_removed_submissions,

        a.first_activity_time as first_activity_time,
        a.last_activity_time as last_activity_time,
        c.first_comment_time as first_comment_time,
        c.last_comment_time as last_comment_time,
        s.first_submission_time as first_submission_time,
        s.last_submission_time as last_submission_time,

        c.first_comment_id as first_comment_id,
        c.last_comment_id as last_comment_id,
        s.first_submission_id as first_submission_id,
        s.last_submission_id as last_submission_id,

        c.comment_score_min,
        c.comment_score_max,
        c.comment_score_sum,
        c.comment_score_avg,

        s.submission_score_min,
        s.submission_score_max,
        s.submission_score_sum,
        s.submission_score_avg,

        a.combined_score_min,
        a.combined_score_max,
        a.combined_score_sum,
        a.combined_score_avg, 


        c.comment_length_min,
        c.comment_length_max,
        c.comment_length_sum,
        c.comment_length_avg,

        s.submission_length_min,
        s.submission_length_max,
        s.submission_length_sum,
        s.submission_length_avg,

        a.combined_length_min,
        a.combined_length_max,
        a.combined_length_sum,
        a.combined_length_avg,

        a.total_score_negative_items,
        a.total_score_zero_items,
        a.total_score_positive_items,

        a.num_sessions

        from (
            select author, subreddit, 
                count(*) as total_activity,
                count(1) filter(where deleted = true) as total_deleted,
                count(1) filter(where removed = true) as total_removed,
                min(created_utc) as first_activity_time, max(created_utc) as last_activity_time,
                min(score) as combined_score_min,
                max(score) as combined_score_max,
                sum(score) as combined_score_sum,
                avg(score) as combined_score_avg,
                min(length) as combined_length_min,
                max(length) as combined_length_max,
                sum(length) as combined_length_sum,
                avg(length) as combined_length_avg,
                count(1) filter(where score < 0) as total_score_negative_items,
                count(1) filter(where score = 0) as total_score_zero_items,
                count(1) filter(where score > 0) as total_score_positive_items,
                count(distinct user_session_id) as num_sessions

            from user_combined_activity
            group by author, subreddit
            order by author asc, subreddit asc
            ) a
        left join user_subreddit_comments_summary c on c.author = a.author and c.subreddit = a.subreddit
        left join user_subreddit_submissions_summary s on s.author = a.author and s.subreddit = a.subreddit

);



create index on user_subreddit_activity (subreddit);
create index on user_subreddit_activity (author);
create index on user_subreddit_activity (subreddit, author);


--
--
--
-- user combined actvity summary
--
--
--
--

drop table if exists user_combined_activity_summary;

create temp table user_combined_activity_summary(
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


        b.subreddits_per_session_min,
        b.subreddits_per_session_max,
        b.subreddits_per_session_avg,

        b.min_activity_in_session,
        b.max_activity_in_session,
        b.avg_activity_in_session,

        b.min_delta_time_in_session,
        b.max_delta_time_in_session,
        b.avg_delta_time_in_session




        from (
                select author, 

                    count(distinct subreddit) as total_subreddits,
                    count(1) filter(where c_id is not NULL) as total_comments,
                    count(1) filter(where s_id is not NULL) as total_submissions,
                    count(*) as total_activity,

                    count(distinct date_trunc('day', created_utc)) as total_active_days,

                    count(1) filter(where deleted is true) as total_deleted,
                    count(1) filter(where c_id is not NULL and deleted is true) as total_deleted_comments,
                    count(1) filter(where s_id is not NULL and deleted is true) as total_deleted_submissions,
                    count(1) filter(where removed is true) as total_removed,
                    count(1) filter(where c_id is not NULL and removed is true) as total_removed_comments,
                    count(1) filter(where s_id is not NULL and removed is true) as total_removed_submissions,

                    min(created_utc) as first_activity_time,
                    max(created_utc) as last_activity_time,
                    min(created_utc) filter(where c_id is not NULL) as first_comment_time,
                    max(created_utc) filter(where c_id is not NULL) as last_comment_time,
                    min(created_utc) filter(where s_id is not NULL) as first_submission_time,
                    max(created_utc) filter(where s_id is not NULL) as last_submission_time,


                    min(score) filter(where c_id is not NULL) as comment_score_min,
                    max(score) filter(where c_id is not NULL) as comment_score_max,
                    sum(score) filter(where c_id is not NULL) as comment_score_sum,
                    avg(score) filter(where c_id is not NULL) as comment_score_avg,

                    min(score) filter(where s_id is not NULL) as submission_score_min,
                    max(score) filter(where s_id is not NULL) as submission_score_max,
                    sum(score) filter(where s_id is not NULL) as submission_score_sum,
                    avg(score) filter(where s_id is not NULL) as submission_score_avg,

                    min(score) as combined_score_min,
                    max(score) as combined_score_max,
                    sum(score) as combined_score_sum,
                    avg(score) as combined_score_avg, 



                    min(length) filter(where c_id is not NULL) as comment_length_min,
                    max(length) filter(where c_id is not NULL) as comment_length_max,
                    sum(length) filter(where c_id is not NULL) as comment_length_sum,
                    avg(length) filter(where c_id is not NULL) as comment_length_avg,

                    min(length) filter(where s_id is not NULL) as submission_length_min,
                    max(length) filter(where s_id is not NULL) as submission_length_max,
                    sum(length) filter(where s_id is not NULL) as submission_length_sum,
                    avg(length) filter(where s_id is not NULL) as submission_length_avg,

                    min(length) as combined_length_min,
                    max(length) as combined_length_max,
                    sum(length) as combined_length_sum,
                    avg(length) as combined_length_avg, 


                    count(1) filter(where score < 0) as total_score_negative_items,
                    count(1) filter(where score = 0) as total_score_zero_items,
                    count(1) filter(where score > 0) as total_score_positive_items,
            
                    max(user_session_id) + 1 as num_sessions

                    from user_combined_activity
                    group by author
                    order by author asc
            ) a
        left join (
            select author, 
                min(subreddit_count) as subreddits_per_session_min,
                max(subreddit_count) as subreddits_per_session_max,
                avg(subreddit_count) as subreddits_per_session_avg,
                min(session_activity_count) as min_activity_in_session,
                max(session_activity_count) as max_activity_in_session,
                avg(session_activity_count) as avg_activity_in_session,
                min(min_delta_time) as min_delta_time_in_session,
                max(max_delta_time) as max_delta_time_in_session,
                avg(avg_delta_time * session_activity_count) as avg_delta_time_in_session
                from (
                    select author, user_session_id, count(distinct subreddit) as subreddit_count,
                        count(*) as session_activity_count,
                        min(user_delta_time) as min_delta_time,
                        max(user_delta_time) as max_delta_time,
                        avg(user_delta_time) filter(where user_delta_time < 3600) as avg_delta_time
                    from user_combined_activity
                    group by author, user_session_id) sps
                group by sps.author
            ) b on b.author = a.author
);




--
--
--
-- user_comments_summary
--
--
--
--

drop table if exists user_comments_summary;

create table user_comments_summary(
    id bigserial,
    author  text,

    total_comments bigint,

    first_comment_time timestamp without time zone,
    last_comment_time timestamp without time zone,
    first_comment_id bigint,
    last_comment_id bigint,

    first_comment_subreddit text,
    last_comment_subreddit text
);


insert into user_comments_summary(
author, 
total_comments,
first_comment_time, last_comment_time,
first_comment_id, last_comment_id,
first_comment_subreddit, last_comment_subreddit
)
(
            select distinct on (author)
                author,
                count(*) over w as total_comments,
                first_value(created_utc) over w as first_comment_time,
                last_value(created_utc) over w as last_comment_time,
                first_value(id) over w as first_comment_id,
                last_value(id) over w as last_comment_id,
                first_value(subreddit) over w as first_comment_subreddit,
                last_value(subreddit) over w as last_comment_subreddit
                from user_comments
                window w as (partition by author order by created_utc, id asc range between unbounded preceding and unbounded following)

);



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






--
--
--
-- only needed for fixing the temp mistake on the linux setup
--
--
--

create table user_comments_summary2 as select * from user_comments_summary;
create table user_submissions_summary2 as select * from user_submissions_summary;

alter table user_comments_summary2 rename to user_comments_summary;
alter table user_submissions_summary2 rename to user_submissions_summary;

create index on user_comments_summary(author);
create index on user_submissions_summary(author);


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



/*insert into user_summary(
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

        case when first_submission_time < first_comment_time then s.first_submission_subreddit
        else c.first_comment_subreddit end as first_activity_subreddit,
        case when last_submission_time > last_comment_time then s.last_submission_subreddit
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


        b.subreddits_per_session_min,
        b.subreddits_per_session_max,
        b.subreddits_per_session_avg,

        b.min_activity_in_session,
        b.max_activity_in_session,
        b.avg_activity_in_session,

        b.min_delta_time_in_session,
        b.max_delta_time_in_session,
        b.avg_delta_time_in_session




        from (
                select author, 

                    count(distinct subreddit) as total_subreddits,
                    count(1) filter(where c_id is not NULL) as total_comments,
                    count(1) filter(where s_id is not NULL) as total_submissions,
                    count(*) as total_activity,

                    count(distinct date_trunc('day', created_utc)) as total_active_days,

                    count(1) filter(where deleted is true) as total_deleted,
                    count(1) filter(where c_id is not NULL and deleted is true) as total_deleted_comments,
                    count(1) filter(where s_id is not NULL and deleted is true) as total_deleted_submissions,
                    count(1) filter(where removed is true) as total_removed,
                    count(1) filter(where c_id is not NULL and removed is true) as total_removed_comments,
                    count(1) filter(where s_id is not NULL and removed is true) as total_removed_submissions,

                    min(created_utc) as first_activity_time,
                    max(created_utc) as last_activity_time,
                    min(created_utc) filter(where c_id is not NULL) as first_comment_time,
                    max(created_utc) filter(where c_id is not NULL) as last_comment_time,
                    min(created_utc) filter(where s_id is not NULL) as first_submission_time,
                    max(created_utc) filter(where s_id is not NULL) as last_submission_time,


                    min(score) filter(where c_id is not NULL) as comment_score_min,
                    max(score) filter(where c_id is not NULL) as comment_score_max,
                    sum(score) filter(where c_id is not NULL) as comment_score_sum,
                    avg(score) filter(where c_id is not NULL) as comment_score_avg,

                    min(score) filter(where s_id is not NULL) as submission_score_min,
                    max(score) filter(where s_id is not NULL) as submission_score_max,
                    sum(score) filter(where s_id is not NULL) as submission_score_sum,
                    avg(score) filter(where s_id is not NULL) as submission_score_avg,

                    min(score) as combined_score_min,
                    max(score) as combined_score_max,
                    sum(score) as combined_score_sum,
                    avg(score) as combined_score_avg, 



                    min(length) filter(where c_id is not NULL) as comment_length_min,
                    max(length) filter(where c_id is not NULL) as comment_length_max,
                    sum(length) filter(where c_id is not NULL) as comment_length_sum,
                    avg(length) filter(where c_id is not NULL) as comment_length_avg,

                    min(length) filter(where s_id is not NULL) as submission_length_min,
                    max(length) filter(where s_id is not NULL) as submission_length_max,
                    sum(length) filter(where s_id is not NULL) as submission_length_sum,
                    avg(length) filter(where s_id is not NULL) as submission_length_avg,

                    min(length) as combined_length_min,
                    max(length) as combined_length_max,
                    sum(length) as combined_length_sum,
                    avg(length) as combined_length_avg, 


                    count(1) filter(where score < 0) as total_score_negative_items,
                    count(1) filter(where score = 0) as total_score_zero_items,
                    count(1) filter(where score > 0) as total_score_positive_items,
            
                    max(user_session_id) + 1 as num_sessions

                    from user_combined_activity
                    group by author
                    order by author asc
            ) a
        left join (
            select author, 
                min(subreddit_count) as subreddits_per_session_min,
                max(subreddit_count) as subreddits_per_session_max,
                avg(subreddit_count) as subreddits_per_session_avg,
                min(session_activity_count) as min_activity_in_session,
                max(session_activity_count) as max_activity_in_session,
                avg(session_activity_count) as avg_activity_in_session,
                min(min_delta_time) as min_delta_time_in_session,
                max(max_delta_time) as max_delta_time_in_session,
                avg(avg_delta_time * session_activity_count) as avg_delta_time_in_session
                from (
                    select author, user_session_id, count(distinct subreddit) as subreddit_count,
                        count(*) as session_activity_count,
                        min(user_delta_time) as min_delta_time,
                        max(user_delta_time) as max_delta_time,
                        avg(user_delta_time) filter(where user_delta_time < 3600) as avg_delta_time
                    from user_combined_activity
                    group by author, user_session_id) sps
                group by sps.author
            ) b on b.author = a.author
        left join (
            select distinct on (author) 
                author,
                first_value(id) over w as first_submission_id,
                last_value(id) over w as last_submission_id,
                first_value(subreddit) over w as first_submission_subreddit,
                last_value(subreddit) over w as last_submission_subreddit
                from user_submissions
                window w as (partition by author order by created_utc, id asc range between unbounded preceding and unbounded following)
            ) s on s.author = a.author
        left join (
            select distinct on (author) 
                author,
                first_value(id) over w as first_comment_id,
                last_value(id) over w as last_comment_id,
                first_value(subreddit) over w as first_comment_subreddit,
                last_value(subreddit) over w as last_comment_subreddit
                from user_comments
                window w as (partition by author order by created_utc, id asc range between unbounded preceding and unbounded following)


            ) c on c.author = a.author


);
*/










-- select author, subreddit, total_deleted, total_removed, total_deleted_old, total_removed_old from 
-- (select author, subreddit, 
--     count(*) as total_activity,
--     count(1) filter(where deleted is true) as total_deleted,
--     count(1) filter(where removed is true) as total_removed,
--     sum(case when deleted is true then 1 else 0 end) as total_deleted_old,
--     sum(case when removed is true then 1 else 0 end) as total_removed_old

-- from user_combined_activity
-- where author in ('Lucky764', 'lucky7club', 'Lucky7Pirate', 'Mcfragger', 'McSkilled', 'QuantumQuetzal', 'QuothTheJess', 'ragn4rok234', 'RamenAvenger', 'RDay', 'rcs_thruster')
-- group by author, subreddit) a

-- where total_deleted + total_removed + total_deleted_old + total_removed_old > 0
-- ;



                select author, 

                    count(distinct subreddit) as total_subreddits,
                    count(1) filter(where c_id is not NULL) as total_comments,
                    count(1) filter(where s_id is not NULL) as total_submissions,
                    count(*) as total_activity,

                    count(distinct date_trunc('day', created_utc)) as total_active_days,

                    count(1) filter(where deleted is true) as total_deleted,
                    count(1) filter(where c_id is not NULL and deleted is true) as total_deleted_comments,
                    count(1) filter(where s_id is not NULL and deleted is true) as total_deleted_submissions,
                    count(1) filter(where removed is true) as total_removed,
                    count(1) filter(where c_id is not NULL and removed is true) as total_removed_comments,
                    count(1) filter(where s_id is not NULL and removed is true) as total_removed_submissions,


                    min(score) filter(where c_id is not NULL) as comment_score_min,
                    max(score) filter(where c_id is not NULL) as comment_score_max,
                    sum(score) filter(where c_id is not NULL) as comment_score_sum,
                    avg(score) filter(where c_id is not NULL) as comment_score_avg,

                    min(score) filter(where s_id is not NULL) as submission_score_min,
                    max(score) filter(where s_id is not NULL) as submission_score_max,
                    sum(score) filter(where s_id is not NULL) as submission_score_sum,
                    avg(score) filter(where s_id is not NULL) as submission_score_avg,

                    min(score) as combined_score_min,
                    max(score) as combined_score_max,
                    sum(score) as combined_score_sum,
                    avg(score) as combined_score_avg, 



                    min(length) filter(where c_id is not NULL) as comment_length_min,
                    max(length) filter(where c_id is not NULL) as comment_length_max,
                    sum(length) filter(where c_id is not NULL) as comment_length_sum,
                    avg(length) filter(where c_id is not NULL) as comment_length_avg,

                    min(length) filter(where s_id is not NULL) as submission_length_min,
                    max(length) filter(where s_id is not NULL) as submission_length_max,
                    sum(length) filter(where s_id is not NULL) as submission_length_sum,
                    avg(length) filter(where s_id is not NULL) as submission_length_avg,

                    min(length) as combined_length_min,
                    max(length) as combined_length_max,
                    sum(length) as combined_length_sum,
                    avg(length) as combined_length_avg, 


                    count(1) filter(where score < 0) as total_score_negative_items,
                    count(1) filter(where score = 0) as total_score_zero_items,
                    count(1) filter(where score > 0) as total_score_positive_items

                    from user_combined_activity
                    where author in ('Lucky764', 'lucky7club', 'Lucky7Pirate', 'Mcfragger', 'McSkilled', 'QuantumQuetzal', 'QuothTheJess', 'ragn4rok234', 'RamenAvenger', 'RDay', 'rcs_thruster')
                    group by author
                    order by author asc;

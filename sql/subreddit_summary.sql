--
--
--
-- subreddit_summary
--
--
--
--

drop table if exists subreddit_summary;

create table subreddit_summary(
    id bigserial,
    sid bigint null,
    name text,

    total_comments bigint,
    total_submissions bigint,
    total_activity bigint,
    unique_authors bigint,

    total_active_days int,  

    total_deleted bigint,
    total_deleted_comments bigint,
    total_deleted_submissions bigint,
    total_removed bigint,
    total_removed_comments bigint,
    total_removed_submissions bigint,

    created_utc timestamp without time zone,

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
    total_score_one_items bigint,
    total_score_positive_gt1_items bigint

 );




insert into subreddit_summary(
sid, name, 
total_comments, total_submissions, total_activity, unique_authors,
total_active_days,
total_deleted, total_deleted_comments, total_deleted_submissions,
total_removed, total_removed_comments, total_removed_submissions,
created_utc,
first_activity_time, last_activity_time, first_comment_time, last_comment_time,
first_submission_time, last_submission_time,
comment_score_min, comment_score_max, comment_score_sum, comment_score_avg,
submission_score_min, submission_score_max, submission_score_sum, submission_score_avg,
combined_score_min, combined_score_max, combined_score_sum, combined_score_avg,
comment_length_min, comment_length_max, comment_length_sum, comment_length_avg,
submission_length_min, submission_length_max, submission_length_sum, submission_length_avg,
combined_length_min, combined_length_max, combined_length_sum, combined_length_avg,
total_score_negative_items, total_score_zero_items, total_score_one_items,
total_score_positive_gt1_items
)
(
    select 
    	s.sid,
    	a.name,
        a.total_comments,
        a.total_submissions,
        a.total_activity,
        a.unique_authors,

        a.total_active_days,

        a.total_deleted,
        a.total_deleted_comments,
        a.total_deleted_submissions,
        a.total_removed,
        a.total_removed_comments,
        a.total_removed_submissions,

        s.created_utc,

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
        a.total_score_one_items,
        a.total_score_positive_gt1_items

        from (
                select 

                	subreddit as name, 

                    count(1) filter(where c_id is not NULL) as total_comments,
                    count(1) filter(where s_id is not NULL) as total_submissions,
                    count(*) as total_activity,
                    count(distinct author) as unique_authors,

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
                    count(1) filter(where score = 1) as total_score_one_items,
                    count(1) filter(where score > 1) as total_score_positive_gt1_items
            
                    from user_combined_activity
                    group by subreddit
            ) a
        left join (
        	select
        		id as sid,
        		display_name,
        		created_utc
        		from subreddits
            ) s on s.display_name = a.name
);


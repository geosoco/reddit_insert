DROP TABLE IF EXISTS user_subreddit_relative_activity;


create table user_subreddit_relative_activity(
    id bigserial,
    author text,
    subreddit text,

    total_activity bigint,
    total_comments bigint,
    total_submissions bigint,
    total_subreddits int,

    total_sub_activity bigint,
    total_sub_comments bigint,
    total_sub_submissions bigint,
    
    sub_activity_ratio numeric(20,6),
    sub_comment_ratio numeric(20,6),
    sub_submission_ratio numeric(20,6),

    first_activity_time timestamp without time zone,
    last_activity_time timestamp without time zone,

    first_sub_activity_time timestamp without time zone,
    last_sub_activity_time timestamp without time zone,    

    first_sub_activity_rel_days int,
    last_sub_activity_rel_days int,
    total_sub_activity_dur_days int,
    total_sub_activity_dur_secs int

 );




insert into user_subreddit_relative_activity (
    author,
    subreddit,

    total_activity,
    total_comments,
    total_submissions,
    total_subreddits,

    total_sub_activity,
    total_sub_comments,
    total_sub_submissions,
    
    sub_activity_ratio,
    sub_comment_ratio,
    sub_submission_ratio,

    first_activity_time,
    last_activity_time,

    first_sub_activity_time,
    last_sub_activity_time,    


    first_sub_activity_rel_days,
    last_sub_activity_rel_days,
    total_sub_activity_dur_days,
    total_sub_activity_dur_secs
)
(
select 
    usa.author,
    usa.subreddit,

    coalesce(ucas.total_activity,0) as total_activity,
    coalesce(ucas.total_comments,0) as total_comments,
    coalesce(ucas.total_submissions,0) as total_submissions,
    ucas.total_subreddits,

    usa.total_activity as total_sub_activity,
    coalesce(usa.total_comments,0) as total_sub_comments, 
    coalesce(usa.total_submissions,0) as total_sub_submissions, 

    usa.total_activity::decimal / ucas.total_activity::decimal as sub_activity_ratio,
    case when coalesce(ucas.total_comments,0) > 0 then coalesce(usa.total_comments,0)::decimal / ucas.total_comments::decimal else 0 end as sub_comment_ratio,
    case when coalesce(ucas.total_submissions,0) > 0 then coalesce(usa.total_submissions,0)::decimal / ucas.total_submissions::decimal else 0 end as sub_submission_ratio,

    ucas.first_activity_time,
    ucas.last_activity_time,
    
    usa.first_activity_time as first_sub_activity,
    usa.last_activity_time as last_sub_activity,
    
    usa.first_activity_time::date - ucas.first_activity_time::date as first_sub_activity_rel_days,
    usa.last_activity_time::date  - ucas.first_activity_time::date as last_sub_activity_rel_days,
    usa.last_activity_time::date - usa.first_activity_time::date as total_sub_activity_dur_days,
    extract(epoch from usa.last_activity_time - usa.first_activity_time) as total_sub_activity_dur
from user_subreddit_activity usa 
left join user_combined_activity_summary ucas on ucas.author = usa.author
);


create index on user_subreddit_relative_activity(author);
create index on user_subreddit_relative_activity(subreddit);
create index on user_subreddit_relative_activity(author,subreddit);


         
DROP TABLE IF EXISTS user_subreddit_daily_summary;

CREATE TABLE user_subreddit_daily_summary
(
    id serial PRIMARY KEY,
    date date,
    author text,
    subreddit text,
    total_items integer,
    num_comments integer,

    num_submissions integer,
    sum_score integer,
    sum_comments_score integer,
    sum_submissions_score integer,

    total_score_negative_items integer,
    total_score_zero_items integer,
    total_score_one_items integer,
    total_score_positive_gt1_items integer,

    total_score_negative_comments integer,
    total_score_zero_comments integer,
    total_score_one_comments integer,
    total_score_positive_gt1_comments integer,

    total_score_negative_submissions integer,
    total_score_zero_submissions integer,
    total_score_one_submissions integer,
    total_score_positive_gt1_submissions integer,


    comment_ratio numeric(20,4)
);




insert into user_subreddit_daily_summary (
date, 
author, 
subreddit, 
total_items, 
num_comments, 
num_submissions, 
sum_score, 
sum_comments_score, 
sum_submissions_score, 
total_score_negative_items, 
total_score_zero_items, 
total_score_one_items, 
total_score_positive_gt1_items, 
total_score_negative_comments, 
total_score_zero_comments, 
total_score_one_comments, 
total_score_positive_gt1_comments, 
total_score_negative_submissions, 
total_score_zero_submissions, 
total_score_one_submissions, 
total_score_positive_gt1_submissions, 
comment_ratio
) (

select
    a.*,
    a.num_comments/a.total_items as comment_ratio
    
    from  (select 
date_trunc('day', created_utc) as date,
author,
subreddit,
count(*) as total_items,
count(*) filter(where c_id is not null) as num_comments,
count(*) filter(where s_id is not null) as num_submissions,
sum(score) as sum_score,
sum(score) filter(where c_id is not null) as sum_comments_score,
sum(score) filter(where s_id is not null) as sum_submissions_score,

count(*) filter(where score < 0) as total_score_negative_items,
count(*) filter(where score = 0) as total_score_zero_items,
count(*) filter(where score = 1) as total_score_one_items,
count(*) filter(where score > 1) as total_score_positive_gt1_items,

count(*) filter(where score < 0 and c_id is not null) as total_score_negative_comments,
count(*) filter(where score = 0 and c_id is not null) as total_score_zero_comments,
count(*) filter(where score = 1 and c_id is not null) as total_score_one_comments,
count(*) filter(where score > 1 and c_id is not null) as total_score_positive_gt1_comments,

count(*) filter(where score < 0 and s_id is not null) as total_score_negative_submissions,
count(*) filter(where score = 0 and s_id is not null) as total_score_zero_submissions,
count(*) filter(where score = 1 and s_id is not null) as total_score_one_submissions,
count(*) filter(where score > 1 and s_id is not null) as total_score_positive_gt1_submissions

from user_combined_activity
group by subreddit, author, date) a
order by subreddit, author, date);



create index on user_subreddit_daily_summary(author);
create index on user_subreddit_daily_summary(subreddit,date);
create index on user_subreddit_daily_summary(subreddit);



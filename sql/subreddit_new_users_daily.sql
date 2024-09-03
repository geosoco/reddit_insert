
                  
                  
DROP TABLE IF EXISTS subreddit_new_users_daily;

CREATE TABLE subreddit_new_users_daily
(
    id serial PRIMARY KEY,
    date date,
    subreddit text,
    total_count integer,
    deleted_count integer,
    removed_count integer,
    deleted_author_count integer,
    automod_count integer,
    unique_authors integer,
    count_new_users integer,
    count_new_submission_users integer,
    count_new_comment_users integer,
    count_single_interaction_users integer,
    count_new_users_single_day integer,
    count_new_users_7_day integer,
    count_new_users_28_day integer,
    count_new_users_gte_29_day integer,
    avg_total_activity_of_new_users numeric(20,4)
);





INSERT INTO subreddit_new_users_daily (
	date, subreddit, total_count, deleted_count, removed_count, deleted_author_count, 
	automod_count, unique_authors,count_new_users, count_new_submission_users, count_new_comment_users, 
	count_single_interaction_users, count_new_users_single_day, count_new_users_7_day, 
	count_new_users_28_day, count_new_users_gte_29_day, avg_total_activity_of_new_users
	)
		select ssd.date::date as ddate,
			ssd.subreddit as subreddit,
			total_count,
			deleted_count,
			removed_count,
			deleted_author_count,
			automod_count,
			unique_authors,
			count_new_users,
			count_new_submission_users,
			count_new_comment_users,
			count_single_interaction_users,
			count_new_users_single_day,
			count_new_users_7_day,
			count_new_users_28_day,
			count_new_users_gte_29_day,
			avg_total_activity_of_new_users
			from subreddit_summary_daily ssd
			full outer join (
				select 
					date_trunc('day', first_activity_time)::date as ddate, 
					subreddit,
					count(*) as count_new_users,
					count(*) filter(where first_activity_time = first_comment_time) as count_new_comment_users,
					count(*) filter(where first_activity_time = first_submission_time) as count_new_submission_users,
					count(*) filter(where total_activity = 1) as count_single_interaction_users,
					count(*) filter(where last_activity_time-first_activity_time < '1 day') as  count_new_users_single_day,
					count(*) filter(where last_activity_time-first_activity_time between '1 day' and '7 day') as  count_new_users_7_day,
					count(*) filter(where last_activity_time-first_activity_time between '8 day' and '28 day') as  count_new_users_28_day,
					count(*) filter(where last_activity_time-first_activity_time >= '29 day') as  count_new_users_gte_29_day,
					avg(total_activity) as avg_total_activity_of_new_users
					from user_subreddit_activity usa 
					group by subreddit, date_trunc('day', first_activity_time)::date) a 
			on a.subreddit=ssd.subreddit and a.ddate=ssd.date
			order by subreddit, ddate asc;



create index on subreddit_new_users_daily(subreddit);
create index on subreddit_new_users_daily(date);

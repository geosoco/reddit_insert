create index user_combined_activity_tmp_author_created_idx on user_combined_activity_tmp(author,created_utc);
cluster user_combined_activity using user_combined_activity_tmp_author_created_idx;
create index on user_combined_activity_tmp(author);
create index on user_combined_activity_tmp(created_utc);
analyze user_combined_activity_tmp;


DROP TABLE IF EXISTS user_combined_activity_tmp2;

CREATE UNLOGGED TABLE user_combined_activity_tmp2
(
    id bigserial,
    c_id bigint null,
    s_id bigint null,
    created_utc timestamp without time zone,
    author text,    
    subreddit text,
    score int,
    length int,
    deleted bool,
    removed bool,
    user_previous_utc timestamp without time zone,
    user_delta_time integer, 
    user_session_id integer
);



insert into user_combined_activity_tmp2 (
		id, c_id, s_id, created_utc, author, subreddit, score, length, deleted, removed,
		user_previous_utc, user_delta_time, user_session_id) (
						select
							id,
							c_id,
							s_id,
							created_utc,
							author,
							subreddit,
							score,
							length,
							deleted,
							removed,
							lag(created_utc) over w as user_previous_utc,
							extract(epoch from created_utc - lag(created_utc,1) over w) as user_delta_time,
							case when extract(epoch from created_utc - lag(created_utc,1) over w) > 3600 then 1 else 0 end as session_boundary
							from user_combined_activity_tmp
							window w as (partition by author order by created_utc asc)

					);



DROP TABLE IF EXISTS user_combined_activity;

CREATE UNLOGGED TABLE user_combined_activity
(
    id bigserial,
    c_id bigint null,
    s_id bigint null,
    created_utc timestamp without time zone,
    author text,    
    subreddit text,
    score int,
    length int,
    deleted bool,
    removed bool,
    user_previous_utc timestamp without time zone,
    user_delta_time integer, 
    user_session_id integer
);


insert into user_combined_activity (
		id, c_id, s_id, created_utc, author, subreddit, score, length, deleted, removed,
		user_previous_utc, user_delta_time, user_session_id) (
			select
				id,
				c_id,
				s_id,
				created_utc,
				author,
				subreddit,
				score,
				length,
				deleted,
				removed,
				user_previous_utc,
				user_delta_time,
				sum(session_boundary) over (partition by author order by created_utc asc) as user_session_id
				from (
						select
							id,
							c_id,
							s_id,
							created_utc,
							author,
							subreddit,
							score,
							length,
							deleted,
							removed,
							lag(created_utc) over w as user_previous_utc,
							extract(epoch from created_utc - lag(created_utc,1) over w) as user_delta_time,
							case when extract(epoch from created_utc - lag(created_utc,1) over w) > 3600 then 1 else 0 end as session_boundary
							from user_combined_activity_tmp
							window w as (partition by author order by created_utc asc)

					) a
		)
;



alter table user_combined_activity add primary key (id);
alter table user_combined_activity set logged;
create index user_combined_activity_author_created_idx on user_combined_activity(author,created_utc);
cluster user_combined_activity using user_combined_activity_author_created_idx;
create index on user_combined_activity(author);
create index on user_combined_activity(created_utc);
create index on user_combined_activity(user_delta_time);
analyze user_combined_activity;




select user_delta_time/60 as mins, count(*) as cnt from user_combined_activity
where user_delta_time < (60*60*24)
group by mins;



DROP TABLE IF EXISTS user_combined_activity_tmp;

CREATE UNLOGGED TABLE user_combined_activity_tmp
(
    id bigserial primary key,
    c_id bigint null,
    s_id bigint null,
    created_utc timestamp without time zone,
    author text,    
    subreddit text,
    score int,
    length int,
    deleted bool,
    removed bool
) tablespace ddrive;


insert into user_combined_activity_tmp
(
	c_id, s_id, created_utc, author, subreddit, score, length, deleted, removed	
)
(
	select id, null, created_utc, author, subreddit, score, length, deleted, removed from user_comments
);

insert into user_combined_activity_tmp
(
	c_id, s_id, created_utc, author, subreddit, score, length, deleted, removed	
)
(
	select null, id, created_utc, author, subreddit, score, length, deleted, removed from user_submissions
);


/*
alter table user_combined_activity add column session_num integer, add column activity_time_delta integer;
*/
create index user_combined_activity_tmp_author_created_idx on user_combined_activity_tmp(author,created_utc);
cluster user_combined_activity using user_combined_activity_author_created_idx;
create index on user_combined_activity_tmp(author);
create index on user_combined_activity_tmp(created_utc);
analyze user_combined_activity_tmp;


/*
select id, author, created_utc, 
lag(created_utc) over w as prev_time, 
extract(epoch from created_utc - lag(created_utc,1) over w) as delta_time,
case when created_utc - lag(created_utc,1) over w > interval '30' minute then 1 else 0 end as session_boundary
from comments_y2008_m03
where created_utc >= '2008-03-01' and created_utc < '2008-03-07'
window w as (partition by author order by created_utc asc); 
*/

drop table if exists user_session_data;

create unlogged table user_session_data (
	id bigint,
	created_utc timestamp without time zone,
	previous_utc timestamp without time zone,
	delta_time integer,
	session_id integer null
);


insert into user_session_data (id, created_utc, previous_utc, delta_time, session_id) (
	select 
		id,
		created_utc,
		lag(created_utc) over w as previous_utc,
		extract(epoch from created_utc - lag(created_utc,1) over w) as delta_time,
		null
from user_combined_activity
window w as (partition by author order by created_utc asc));


update user_session_data usd
	set usd.session_id = a.session_id
	from (
		select id, case when delta_time > 3600 then 1 else 0 end as session_border

		) 


update user_combined_activity uca
	set activity_time_delta = b.delta_time,
	session_boundary_60min = case when b.delta_time >= (60*60)  then true else false end
	from user_session_data b
	where b.id = uca.id;
/* QUERY IS SLOW AS FUCK */



DROP TABLE IF EXISTS user_combined_activity;

CREATE UNLOGGED TABLE user_combined_activity
(
    id bigserial primary key,
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

/*
select k, percentile_disc(k) within group (order by delta_time)
from user_session_data, generate_series(0.01, 1, 0.01) as k
group by k


select k, percentile_disc(k) within group (order by delta_time)
from user_session_data usd, generate_series(0.00, 1, 0.1) as k, user_combined_activity uca
where delta_time > 300 and delta_time < 24*60*60
and uca.id = usd.id
and uca.author not in ('AutoModerator', '[deleted]')
group by k;



  k   │ percentile_disc
══════╪═════════════════
 0.00 │             301
 0.10 │             475
 0.20 │             745
 0.30 │            1233
 0.40 │            2180
 0.50 │            4041
 0.60 │            7608
 0.70 │           14917
 0.80 │           31673
 0.90 │           55038
 1.00 │           86399
(11 rows)

Time: 58212669.860 ms (16:10:12.670)
*/


SELECT *, pg_size_pretty(total_bytes) AS total
    , pg_size_pretty(index_bytes) AS INDEX
    , pg_size_pretty(toast_bytes) AS toast
    , pg_size_pretty(table_bytes) AS TABLE
  FROM (
  SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
      SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
              , c.reltuples AS row_estimate
              , pg_total_relation_size(c.oid) AS total_bytes
              , pg_indexes_size(c.oid) AS index_bytes
              , pg_total_relation_size(reltoastrelid) AS toast_bytes
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE relkind = 'r' and relname ilike '%user%'
  ) a
) a;



SELECT schemaname, relname, n_live_tup, n_dead_tup, last_autovacuum
FROM pg_stat_all_tables
where relname like '%user%'
ORDER BY n_dead_tup
    / (n_live_tup
       * current_setting('autovacuum_vacuum_scale_factor')::float8
          + current_setting('autovacuum_vacuum_threshold')::float8)
     DESC;
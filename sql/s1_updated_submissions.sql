
drop table if exists s1_updated_submissions;

create table s1_updated_submissions(
	id bigint primary key,
	created_utc timestamp without time zone,
	author text,
	subreddit text,
	score int,
	ups int,
	downs int,
	upvote_ratio decimal,
	num_comments int,	
	data jsonb
);

grant all on s1_updated_submissions to reddit_insert;
grant select on s1_updated_submissions to public;

drop table if exists submission_engagement_raw;

select 
	coalesce(s.id, sacc.article) as id, 
	s.subreddit,
	s.created_utc,
	(s.data->>'score')::int as score, 
	(s.data->>'num_comments')::int as num_comments_in_json,
	coalesce(sacc.comment_count, 0) as comment_count_from_data,
	coalesce(sacc.automod_count, 0) as automod_comment_count
into submission_engagement_raw
from submissions s
full outer join submissions_actual_comment_counts sacc on sacc.article = s.id;


grant select on submission_engagement_raw to public;

create index on submission_engagement_raw(id);
create index on submission_engagement_raw(subreddit);
create index on submission_engagement_raw(score);
create index on submission_engagement_raw(comment_count_from_data);


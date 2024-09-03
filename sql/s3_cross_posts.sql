--
--
-- s3_cross_posts
--
--

drop table if exists s3_cross_posts;




select
	sl.subreddit, sl.id, sl.created_utc, sl.author, sl.title, sl.score, sl.num_comments_from_data, sl.domain, sl.url, sl.source_id36,
	sl.source_id, 
	s.subreddit as source_subreddit, s.author as source_author, s.created_utc as source_created_utc, s.data->>'score' as source_score
into s3_cross_posts
from s3_cross_posts_intermediate sl
left join submissions s on s.id = sl.source_id;



grant select on s3_cross_posts to public;
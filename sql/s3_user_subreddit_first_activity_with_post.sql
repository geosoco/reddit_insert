--
--
-- s3_user_subreddit_first_activity_with_post
--
--
--
--




select 
usa.id, usa.author, usa.subreddit, usa.first_activity_time, usa.total_activity, usa.first_comment_id, c.article
into s3_user_subreddit_first_activity_with_post
from user_subreddit_activity usa
left join comments c on c.id = usa.first_comment_id
where usa.subreddit in ('The_Donald', 'SandersForPresident', 'hillaryclinton') and first_activity_time = first_comment_time


grant select on s3_user_subreddit_first_activity_with_post to public;


create index on s3_user_subreddit_first_activity_with_post(author);
create index on s3_user_subreddit_first_activity_with_post(author, subreddit);
create index on s3_user_subreddit_first_activity_with_post(article);
--
-- submissions_actual_comment_counts
--

select c.article, count(*) as comment_count, count(*) filter (where author = 'AutoModerator') as automod_count
into submissions_actual_comment_counts
from comments c
group by c.article


grant select on submissions_actual_comment_counts to public;


create index on submissions_actual_comment_counts(article);
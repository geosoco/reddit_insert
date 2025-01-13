--
--
-- s3_crossposts_inbound
--
--
--
--

drop table if exists s3_crossposts_inbound;

select
*
into s3_crossposts_inbound
from cross_posts_intermediate
where source_id is not null and source_subreddit in ('The_Donald', 'hillaryclinton', 'SandersForPresident');


grant select on s3_crossposts_inbound to public;
--
--
-- s3_cross_posts_intermediate
--
--

drop table if exists s3_cross_posts_intermediate;


with short_links_raw as (
select
		subreddit, id, created_utc, author, title, score, num_comments_from_data, domain, url, 
		null as source_subreddit,
		(regexp_match(url, '(?<!//i\.)redd.it/([\w\d]{5,6})$'))[1] as source_id36
	from s3_political_submissions
where domain ~* '^(redd\.it|np\.redd\.it)' 
),
short_links as (
select 
	slr.*,
	case when source_id36 is not null and length(source_id36) > 4 then base36_decode(source_id36) else null end as source_id
	from short_links_raw slr
),
cross_posts_raw as (
	select subreddit, id, created_utc, author, title, score, num_comments_from_data, domain, url,
			(regexp_match(url, 'reddit.com/r/([a-zA-Z0-9_\-]+)'))[1] as source_subreddit,
			(regexp_match(url, 'reddit.com/r/([a-zA-Z0-9_\-]+)/comments/([a-z0-9]{4,7})'))[2] as source_id36
	
	from s3_political_submissions
	where domain ~* '^(np\.)?reddit.com'
),
cross_posts as (
	select
		cpr.*,
		case when source_id36 is not null and length(source_id36) > 4 then base36_decode(source_id36) else null end as source_id
	from cross_posts_raw cpr
	--where source_id36 is not null and length(source_id36) > 4
),
combined as (
	select
		*
	from short_links
	union all
	select 
		* 
	from cross_posts
)
select
* 
into s3_cross_posts_intermediate
from combined;


grant select on s3_cross_posts_intermediate to public;
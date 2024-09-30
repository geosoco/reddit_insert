--
--
-- cross_posts_intermediate
--
--

drop table if exists cross_posts_intermediate;




with possible_cross_posts as (
	select
		*
	from submissions s
	where s.data->>'domain' ~* '^(redd\.it|np\.redd\.it|reddit\.com|np\.reddit\.com)'
),

short_links_raw as (
select
		subreddit, id, created_utc, author, data->>'title' as title, data->>'score' as score, 
		data->>'num_comments' as num_comments_from_data, data->>'domain' as domain, data->>'url' as url, 
		null as source_subreddit,
		(regexp_match(data->>'url', '(?<!//i\.)redd.it/([\w\d]{5,6})$'))[1] as source_id36
	from possible_cross_posts pcp
where data->>'domain' ~* '^(redd\.it|np\.redd\.it)' 
),
short_links as (
select 
	slr.*,
	case when source_id36 is not null and length(source_id36) > 4 then base36_decode(source_id36) else null end as source_id
	from short_links_raw slr
),
short_links_resolved as (
	select
		sl.subreddit, sl.id, sl.created_utc, sl.author, sl.title, sl.score, 
		sl.num_comments_from_data, sl.domain, sl.url, 
		s.subreddit as source_subreddit,
		sl.source_id36,
		sl.source_id
	from short_links sl
	left join submissions s on s.id = sl.source_id
),
cross_posts_raw as (
	select subreddit, id, created_utc, author, data->>'title' as title, data->>'score' as score, 
		data->>'num_comments' as num_comments_from_data, data->>'domain' as domain, data->>'url' as url,
			(regexp_match(data->>'url', 'reddit.com/r/([a-zA-Z0-9_\-]+)'))[1] as source_subreddit,
			(regexp_match(data->>'url', 'reddit.com/r/([a-zA-Z0-9_\-]+)/comments/([a-z0-9]{4,7})'))[2] as source_id36
	
	from possible_cross_posts pcp
	where data->>'domain' ~* '^(np\.)?reddit.com' and (regexp_match(data->>'url', 'reddit.com/r/([a-zA-Z0-9_\-]+)/comments/([a-z0-9]{4,7})') is not null)
),
cross_posts as (
	select
		cpr.*,
		case when cpr.source_id36 is not null and length(cpr.source_id36) > 4 then base36_decode(cpr.source_id36) else null end as source_id
	from cross_posts_raw cpr
	--where source_id36 is not null and length(source_id36) > 4
),
combined as (
	select
		*
	from short_links_resolved
	union all
	select 
		* 
	from cross_posts
)
select
* 
into cross_posts_intermediate
from combined
order by created_utc asc;






grant select on cross_posts_intermediate to public;

create index on cross_posts_intermediate(subreddit);
create index on cross_posts_intermediate(source_subreddit);
create index on cross_posts_intermediate(created_utc);
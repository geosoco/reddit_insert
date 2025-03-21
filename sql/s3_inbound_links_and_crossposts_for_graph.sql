--
-- s3_inbound_links_and_crossposts_for_graph
--
--
--





with links as (
select
*,
(regexp_match(mentioned_sub_link, '([a-zA-Z0-9_\-]+)/comments/([a-z0-9]{4,7})', 'i'))[2] as target_id36,
base36_decode((regexp_match(mentioned_sub_link, '([a-zA-Z0-9_\-]+)/comments/([a-z0-9]{4,7})', 'i'))[2])::bigint as target_id
from s3_inbound_mentions_combined_flat
where link_type = 'link' and mentioned_sub_link ~* '([a-zA-Z0-9_\-]+)/comments/([a-z0-9]{4,7})'

),
cross_posts as (
	select
		cast(null as bigint) as comment_id,
		id as submission_id,
		source_subreddit as mentioned_sub_name,
		subreddit,
		created_utc,
		cast(null as bigint) as creation_delta_days,
		author,
		'crosspost' as link_type,
		url as mentioned_sub_link,
		's' as source,
		null as sub_source,
		source_id36 as target_id36,
		source_id::bigint as target_id
	from s3_crossposts_inbound
	
), combined as (
select * from links
union all 
select * from cross_posts
)
select *
into s3_inbound_links_and_crossposts_for_graph
from combined;


grant select on s3_inbound_links_and_crossposts_for_graph to public;


create index on s3_inbound_links_and_crossposts_for_graph(mentioned_sub_name);
create index on s3_inbound_links_and_crossposts_for_graph(target_id);
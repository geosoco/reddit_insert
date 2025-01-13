--
--
-- s3_inbound_adv_content_combined
--
--
--
--
--

drop table if exists s3_inbound_adv_content_combined;

with full_promos as (
select
	comment_id,
	submission_id,
	mentioned_sub_name,
	subreddit,
	created_utc,
	author,
	link_type,
	mentioned_sub_link as url
from s3_inbound_mentions_combined_flat imcf

),
valid_crossposts as (
	select
		cast(null as bigint) as comment_id,
		id as submission_id,
		source_subreddit as mentioned_sub_name,
		subreddit,
		created_utc,
		author,
		'crosspost' as link_type,
		url 


	from s3_crossposts_inbound sci
	where source_subreddit is not null 
),
combined as (
select * from full_promos fp
union all
select * from valid_crossposts vc
),
dupes_check as (
select
	comment_id,
	submission_id,
	created_utc,
	author,
	mentioned_sub_name,
	subreddit,
	count(*) as total_promos,
	count(*) filter (where link_type = 'link') as num_links,
	count(distinct url) filter (where link_type = 'link') as num_distinct_links,
	count(*) filter (where link_type = 'mention') as num_mentions,
	count(distinct url) filter (where link_type = 'mention') as num_unique_mentions,
	count(*) filter (where link_type = 'crosspost') as num_crossposts,
	count(distinct url) filter (where link_type = 'crosspost') as num_unique_crossposts,
	
	array_agg(link_type) as link_types_array,
	array_agg(url) as urls_array
	
from combined
group by comment_id, submission_id, created_utc, author, mentioned_sub_name, subreddit
)

select
	row_number() over () as id,
	*
into s3_inbound_adv_content_combined
from dupes_check;

grant select on s3_inbound_adv_content_combined to public;

create index on s3_inbound_adv_content_combined (created_utc);
create index on s3_inbound_adv_content_combined(mentioned_sub_name);
--
--
-- s2_submission_title_sub_link_details
--
--

drop table if exists s2_submission_title_sub_link_details;


with lcase as (
	select 
		id, created_utc, author, subreddit, 
		lower(trim(both '/' from mentioned_sub_link)) as mentioned_sub_link
	from s2_submission_title_mentions_full
),
total_link_counts as (
	select id, count(*) as total_links_count
	from s2_submission_title_mentions_full
	group by id
),
unique_links as (
select
	id, mentioned_sub_link, count(*) as unique_links_count
	from lcase
	group by id, mentioned_sub_link
),
link_types as (
	select 
		*, 
		case when (strpos(mentioned_sub_link, '/') > 0) then 'link' else 'mention' end as link_type
	from unique_links
),
aggregated as (
	select 
		s1cc.*, 
		case when length(lower(split_part(s1cc.mentioned_sub_link, '/', 1))) <= 23 
			then lower(split_part(s1cc.mentioned_sub_link, '/', 1)) 
			else NULL end as mentioned_sub_name,
		case when length(lower(split_part(s1cc.mentioned_sub_link, '/', 1))) > 23 
			then lower(split_part(s1cc.mentioned_sub_link, '/', 1)) 
			else NULL end as invalid_mentioned_sub_name,
		case when length(lower(split_part(s1cc.mentioned_sub_link, '/', 1))) > 23 then TRUE else FALSE end as is_invalid_mentioned,
		(lower(s1cc.subreddit) = lower(split_part(s1cc.mentioned_sub_link, '/', 1)))::boolean as self_reference,
		tlc.total_links_count,
		lt.link_type,
		lt.unique_links_count,
		subs.created_utc as sub_creation_date,
		(case when subs.created_utc is not null then s1cc.created_utc > subs.created_utc else false end) as sub_exists_at_mention,
		length(s1.data->>'title') as title_length,
		s1.data->>'title' as title
	from s2_submission_title_mentions_full s1cc
	left join total_link_counts tlc on tlc.id = s1cc.id
	left join link_types lt on lt.id = s1cc.id and lt.mentioned_sub_link = lower(rtrim(s1cc.mentioned_sub_link, '/'))
	left join submissions s1 on s1.id = s1cc.id
	left join subreddits subs on lower(subs.display_name) = lower(split_part(s1cc.mentioned_sub_link, '/', 1))
)
select * 
into s2_submission_title_sub_link_details
from aggregated;


grant select on s2_submission_title_sub_link_details to public;


create index on s2_submission_title_sub_link_details(subreddit);
create index on s2_submission_title_sub_link_details(created_utc);
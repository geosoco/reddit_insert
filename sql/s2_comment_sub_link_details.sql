--
-- s2_comment_sub_link_details
--
-- summary table for s2  comment subreddit links
--
-- depends on s2_comment_sub_link_details
--
--

drop table if exists s2_comment_sub_link_details;


with lcase as (
	select 
		id, created_utc, author, subreddit, 
		lower(trim(both '/' from mentioned_sub_link)) as mentioned_sub_link
	from s2_comment_mentions_full
),
total_link_counts as (
	select id, count(*) as total_links_count
	from s2_comment_mentions_full
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
		lower(split_part(s1cc.mentioned_sub_link, '/', 1)) as mentioned_sub_name,
		(lower(s1cc.subreddit) = lower(split_part(s1cc.mentioned_sub_link, '/', 1)))::boolean as self_reference,
		tlc.total_links_count,
		lt.link_type,
		lt.unique_links_count,
		subs.created_utc as sub_creation_date,
		(case when subs.created_utc is not null then s1cc.created_utc > subs.created_utc else false end) as sub_exists_at_mention,
		length(c1.data->>'body') as body_length,
		c1.data->>'body' as body
	from s2_comment_mentions_full s1cc
	left join total_link_counts tlc on tlc.id = s1cc.id
	left join link_types lt on lt.id = s1cc.id and lt.mentioned_sub_link = lower(rtrim(s1cc.mentioned_sub_link, '/'))
	left join comments c1 on c1.id = s1cc.id
	left join subreddits subs on lower(subs.display_name) = lower(split_part(s1cc.mentioned_sub_link, '/', 1))
)
select * 
into s2_comment_sub_link_details
from aggregated;


grant select on s2_comment_sub_link_details to public;


create index on s2_comment_sub_link_details(subreddit);
create index on s2_comment_sub_link_details(created_utc);
-- the following fails in the above example because there are a lot of invalid mentioned sub names in here, and some that are too long
-- for the indexes used

-- this code is added later because of hte message above
-- some of these invalid links are feedback mentions like r/whythehellisthisathing
-- some are multi-subreddits so they're a bunch of actual subreddits separated by a + (rem+ELI5+science)
-- and there's just a bunch of misdetected links from spam or other links
-- so these commands add two additional columsn and move invalid links to the other column
-- and set a flag
-- then zero out the mentioned subreddit
-- NOTE: the length > 22 comes from looking at the subreddit data. It should probably be 23, but...
-- There's a few subreddits, like user subreddits
-- that have a length of 23 as they're represented as u_subredditname, so I think they're hitting the 21 character limit
-- and appending the u_ in front. However, I think this is only an internal representation of it
--
alter table s2_comment_sub_link_details
	add column invalid_mentioned_sub_name text default null,
	add column is_invalid_mentioned bool default null;



update s2_comment_sub_link_details
set invalid_mentioned_sub_name = mentioned_sub_name,
is_invalid_mentioned = TRUE
where length(mentioned_sub_name) > 23;


update s2_comment_sub_link_details set mentioned_sub_name = NULL where length(mentioned_sub_name) > 23;


create index on s2_comment_sub_link_details(mentioned_sub_name);
create index on s2_comment_sub_link_details(author);
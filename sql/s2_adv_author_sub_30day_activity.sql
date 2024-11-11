--
-- s2_adv_author_sub_30day_activity
-- 
--
-- user subreddit combined advertising
-- 
--
--
--


drop table if exists s2_adv_author_sub_30day_activity;

with mac_subs as (
	select
		distinct subreddit
	from s2_mods_and_creators
),
year_subs as (
	select display_name as subreddit, created_utc
	from mac_subs ms
	left join subreddits s on ms.subreddit = s.display_name
),
comment_ads as (
	select
		ys.subreddit as mentioned_sub_name, 
		author, 
		id,
		sld.created_utc,
		(extract(epoch from (sld.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
		count(distinct case when link_type = 'link' then id else NULL end) as num_link_comments,
		count(distinct case when link_type = 'mention' then id else NULL end) as num_mention_comments
		
	from year_subs ys
	left join s2_comment_sub_link_details sld on sld.mentioned_sub_name = lower(ys.subreddit)
	where self_reference != TRUE 
	group by ys.subreddit, author, id, sld.created_utc, (extract(epoch from (sld.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int
),
submission_ads as (
	select 
		ys.subreddit as mentioned_sub_name,
		author,
		id,
		(extract(epoch from (ascc.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
		sum(num_selftext_mentions) as num_sub_selftext_mentions,
		sum(num_selftext_links) as num_sub_selftext_links,
		sum(num_title_mentions) as num_sub_title_mentions,
		sum(num_title_links) as num_sub_title_links,
		sum(num_cross_post) as num_cross_posts
		
	from year_subs ys
	left join s2_adv_submissions_counts_combined ascc on ascc.mentioned_subreddit = ys.subreddit
	group by ys.subreddit, author, id, (extract(epoch from (ascc.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int
),
combined as (
 
select 
	ca.mentioned_sub_name,
	ca.creation_delta_months,
	ca.author,

	ca.id as comment_id,
	coalesce(ca.num_link_comments, 0) as num_comment_links,
	coalesce(ca.num_mention_comments, 0) as num_comment_mentions,

	NULL as submission_id,
	0 as num_sub_selftext_mentions,
	0 as num_sub_selftext_links,
	0 as num_sub_title_mentions,
	0 as num_sub_title_links,
	0 as num_cross_posts	

	from comment_ads ca

	union all

select
	sa.mentioned_sub_name,
	sa.creation_delta_months,
	sa.author,

	NULL as comment_id,
	0 as num_comment_links,
	0 as num_comment_mentions,
	
	sa.id as submission_id,
	coalesce(sa.num_sub_selftext_mentions, 0) as  num_sub_selftext_mentions,
	coalesce(sa.num_sub_selftext_links, 0) as  num_sub_selftext_links,
	coalesce(sa.num_sub_title_mentions, 0) as  num_sub_title_mentions,
	coalesce(sa.num_sub_title_links, 0) as num_sub_title_links,
	coalesce(sa.num_cross_posts, 0) as num_cross_posts
from submission_ads sa
)
select
	mentioned_sub_name,
	creation_delta_months,
	author,

	sum(case when comment_id is not null then 1 else 0 end) as num_comment_refs,
	sum(num_comment_links) as num_comment_links,
	sum(num_comment_mentions) as num_comment_mentions,

	sum(case when submission_id is not null then 1 else 0 end) as num_submission_refs,
	sum(num_sub_selftext_mentions) as  num_sub_selftext_mentions,
	sum(num_sub_selftext_links) as  num_sub_selftext_links,
	sum(num_sub_title_mentions) as  num_sub_title_mentions,
	sum(num_sub_title_links) as num_sub_title_links

into s2_adv_author_sub_30day_activity
from combined
group by mentioned_sub_name, creation_delta_months, author;


create index on s2_adv_author_sub_30day_activity(mentioned_sub_name);
create index on s2_adv_author_sub_30day_activity(creation_delta_months);
create index on s2_adv_author_sub_30day_activity(author);
create index on s2_adv_author_sub_30day_activity(mentioned_sub_name, author);

grant select on s2_adv_author_sub_30day_activity to public;

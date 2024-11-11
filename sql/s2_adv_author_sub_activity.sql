--
-- s2_adv_author_sub_activity
--
--
-- user subreddit combined advertising
-- 
--
--
--


drop table if exists s2_adv_author_sub_activity;


with year_subs as (
	select 
		display_name as subreddit, 
		created_utc
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
),
comment_ads as (
	select
		ys.subreddit as mentioned_sub_name, 
		author, 
		count(distinct id) as num_comment_refs,
		count(distinct case when link_type = 'link' then id else NULL end) as num_link_comments,
		count(distinct case when link_type = 'mention' then id else NULL end) as num_mention_comments
		
	from year_subs ys
	left join s2_comment_sub_link_details sld on sld.mentioned_sub_name = lower(ys.subreddit)
	where self_reference != TRUE 
	group by ys.subreddit, author
),
submission_ads as (
	select 
		ys.subreddit as mentioned_sub_name,
		author,
		count(distinct id) as num_submission_refs,
		sum(num_selftext_mentions) as num_sub_selftext_mentions,
		sum(num_selftext_links) as num_sub_selftext_links,
		sum(num_title_mentions) as num_sub_title_mentions,
		sum(num_title_links) as num_sub_title_links
		
	from year_subs ys
	left join s2_adv_submissions_counts_combined ascc on ascc.mentioned_subreddit = ys.subreddit
	group by ys.subreddit, author
)
select 
	coalesce(ca.mentioned_sub_name, sa.mentioned_sub_name) as mentioned_sub_name,
	coalesce(ca.author, sa.author) as author,
	
	coalesce(ca.num_comment_refs, 0) as num_comment_refs,
	coalesce(ca.num_link_comments, 0) as num_comment_links,
	coalesce(ca.num_mention_comments, 0) as num_comment_mentions,

	coalesce(sa.num_submission_refs, 0) as num_submission_refs,
	coalesce(sa.num_sub_selftext_mentions, 0) as  num_sub_selftext_mentions,
	coalesce(sa.num_sub_selftext_links, 0) as  num_sub_selftext_links,
	coalesce(sa.num_sub_title_mentions, 0) as  num_sub_title_mentions,
	coalesce(sa.num_sub_title_links, 0) as num_sub_title_links
	
into s2_adv_author_sub_activity
from comment_ads ca
full join submission_ads sa on sa.mentioned_sub_name = ca.mentioned_sub_name and sa.author = ca.author;


create index on s2_adv_author_sub_activity(mentioned_sub_name);
create index on s2_adv_author_sub_activity(author);
create index on s2_adv_author_sub_activity(mentioned_sub_name, author);

grant select on s2_adv_author_sub_activity to public;

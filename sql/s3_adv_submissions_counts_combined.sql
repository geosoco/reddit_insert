--
-- s3_adv_submissions_counts_combined
--
-- advertising counts by user and sub
--
--
--



drop table if exists s3_adv_submissions_counts_combined;

with year_subs as (
	select ss.name as subreddit, s.created_utc
	from subreddit_summary ss
	left join subreddits s on s.display_name = ss.name
	where total_activity >= 400 and unique_authors >= 10
)
,
crossposts as (
select 
	ys.subreddit as source_subreddit,
	cpi.subreddit as target_subreddit,
	cpi.id,
	cpi.author,
	cpi.created_utc
from year_subs ys
inner join cross_posts_intermediate cpi on lower(cpi.source_subreddit) = lower(ys.subreddit)
where source_subreddit is not null and lower(cpi.source_subreddit) != lower(cpi.subreddit)
),
title_links as (
	select 
		a.id,
		a.mentioned_sub_name,
		stsld.subreddit,
		a.author, 
		a.created_utc,
		--stsld.self_reference,
		sum(case when link_type = 'mention' then 1 else 0 end) as num_mentions,
		sum(case when link_type = 'link' then 1 else 0 end) as num_links

	from (
		select
			distinct stsld.id, ys.subreddit as mentioned_sub_name, author, stsld.created_utc
		from year_subs ys 
		inner join s2_submission_title_sub_link_details stsld on lower(ys.subreddit) = lower(stsld.mentioned_sub_name)
		where self_reference = false
	) a
	left join s2_submission_title_sub_link_details stsld on stsld.id = a.id and lower(a.mentioned_sub_name) = lower(stsld.mentioned_sub_name)
	where self_reference = false
	group by a.id, a.mentioned_sub_name, stsld.subreddit, a.author, a.created_utc
),
selftext_links as (
	select 
		a.id,
		a.mentioned_sub_name,
		sssld.subreddit,
		a.author, 
		a.created_utc,
		--sssld.self_reference,
		sum(case when link_type = 'mention' then 1 else 0 end) as num_mentions,
		sum(case when link_type = 'link' then 1 else 0 end) as num_links

	from (
		select
			distinct sssld.id, ys.subreddit as mentioned_sub_name, author, sssld.created_utc
		from year_subs ys 
		inner join s2_submission_selftext_sub_link_details sssld on lower(ys.subreddit) = lower(sssld.mentioned_sub_name)
		where self_reference = false
	) a
	left join s2_submission_selftext_sub_link_details sssld on sssld.id = a.id and lower(a.mentioned_sub_name) = lower(sssld.mentioned_sub_name)
	where self_reference = false
	group by a.id, a.mentioned_sub_name, sssld.subreddit, a.author, a.created_utc
),
submission_selftext_and_title_links as (
	select
		coalesce(stl.id, ssl.id) as id, 
		coalesce(stl.mentioned_sub_name, ssl.mentioned_sub_name) as mentioned_sub_name,
		coalesce(stl.subreddit, ssl.subreddit) as subreddit,
		coalesce(stl.author, ssl.author) as author,
		coalesce(stl.created_utc, ssl.created_utc) as created_utc,
		coalesce(ssl.num_mentions,0) as num_selftext_mentions,
		coalesce(ssl.num_links,0) as num_selftext_links,
		coalesce(stl.num_mentions, 0) as num_title_mentions,
		coalesce(stl.num_links, 0) as num_title_links
	from title_links stl
	full outer join selftext_links ssl on ssl.id = stl.id
)
select
	coalesce(sl.id, cp.id) as id,
	coalesce(sl.mentioned_sub_name, cp.source_subreddit) as mentioned_subreddit,
	coalesce(sl.subreddit, cp.target_subreddit) as subreddit,
	sl.id as sublink_id,
	cp.id as crosspost_id,
	coalesce(sl.author, cp.author) as author,
	coalesce(sl.created_utc, cp.created_utc) as created_utc,
	coalesce(sl.num_selftext_mentions, 0) as num_selftext_mentions,
	coalesce(sl.num_selftext_links, 0) as num_selftext_links,
	coalesce(sl.num_title_mentions, 0) as num_title_mentions,
	coalesce(sl.num_title_links, 0) as num_title_links,
	case when cp.id is not null then 1 else 0 end as num_cross_post

into s3_adv_submissions_counts_combined
from crossposts cp
full join submission_selftext_and_title_links sl on sl.id = cp.id;

create index on s3_adv_submissions_counts_combined(id);
create index on s3_adv_submissions_counts_combined(mentioned_subreddit);
create index on s3_adv_submissions_counts_combined(author);

grant select on s3_adv_submissions_counts_combined to public;

	
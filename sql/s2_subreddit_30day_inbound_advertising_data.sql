----------------------------------------
--
-- s2_subreddit_30day_inbound_advertising_data
--
----------------------------------------



drop table if exists s2_subreddit_30day_inbound_advertising_data;

with year_subs as (
	select 
		display_name, 
		created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
),
sub_months as (
	select 
	ys.display_name as subreddit, 
	created_utc,
	max_months,
	month as creation_delta_months

	from year_subs ys
	cross join lateral generate_series(0, floor(ys.max_months)::int) m(month)
	order by ys.display_name, month
),
comment_ads as (
	select
		ys.display_name as mentioned_sub_name,
		(extract(epoch from (sld.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
		count(distinct author) as num_authors,
		count(distinct case when author != '[deleted]' then author else NULL end) as non_deleted_authors,
		count(distinct subreddit) as num_subreddits,
		count(*) as total_links_or_mentions,
		count(distinct id) as total_comment_adv_comments,

		count(distinct case when link_type = 'link' then id else NULL end) as link_comments,
		count(distinct case when link_type = 'mention' then id else NULL end) as mention_comments,
		
		count(distinct case when link_type = 'link' then subreddit else NULL end) as link_subreddits,
		count(distinct case when link_type = 'mention' then subreddit else NULL end) as mention_subreddits,
	
		count(distinct case when link_type = 'link' then author else NULL end) as link_authors,
		count(distinct case when link_type = 'mention' then author else NULL end) as mention_authors,
	
		sum(case when link_type = 'link' then 1 else 0 end ) as total_links,
		sum(case when link_type = 'mention' then 1 else 0 end ) as total_mentions

	
		
	from year_subs ys
	left join s2_comment_sub_link_details sld on sld.mentioned_sub_name = lower(ys.display_name)
	where self_reference != TRUE 
	group by ys.display_name, creation_delta_months
),
submission_links as (
select
	a.id,
	a.mentioned_sub_name,
	(extract(epoch from (a.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
	coalesce(stsld.subreddit, sssld.subreddit) as subreddit,
	coalesce(stsld.created_utc, sssld.created_utc) as created_utc,
	coalesce(stsld.author, sssld.author) as author,

	coalesce(stsld.unique_links_count,0) as title_unique_links_count,
	coalesce(sssld.unique_links_count,0) as selftext_unique_links_count,

	stsld.link_type as title_link_type,
	sssld.link_type as selftext_link_type
	
	from year_subs ys
	left join ( 
		select
			distinct stsld.id, ys.display_name as mentioned_sub_name, stsld.created_utc
		from year_subs ys 
		left join s2_submission_title_sub_link_details stsld on lower(ys.display_name) = stsld.mentioned_sub_name
		union all
		select
			distinct sssld.id, ys.display_name as mentioned_sub_name, sssld.created_utc
		from year_subs ys 
		left join s2_submission_selftext_sub_link_details sssld on lower(ys.display_name) = sssld.mentioned_sub_name
	) a	on a.mentioned_sub_name = ys.display_name
	left join s2_submission_title_sub_link_details stsld on stsld.id = a.id
	left join s2_submission_selftext_sub_link_details sssld on sssld.id = a.id
),
submissions_summarized as (
	select
		sm.subreddit,
		sm.creation_delta_months,
		count(distinct sl.subreddit) as total_subreddits,
		count(distinct sl.id) as total_submissions,
		count(distinct sl.author) as total_authors,
		count(case when author != '[deleted]' then author else NULL end) as non_deleted_authors,
	
		count(distinct case when title_unique_links_count > 0 then sl.id else NULL end) as distinct_title_submissions,
		count(distinct case when selftext_unique_links_count > 0 then sl.id else NULL end) as distinct_selftext_submissions,
		
		count(distinct case when title_unique_links_count > 0 then sl.subreddit else NULL end) as distinct_title_subreddits,
		count(distinct case when selftext_unique_links_count > 0 then sl.subreddit else NULL end) as distinct_selftext_subreddits,
	
		count(distinct case when title_unique_links_count > 0 then author else NULL end) as distinct_title_authors,
		count(distinct case when selftext_unique_links_count > 0 then author else NULL end) as distinct_selftext_authors,
	
		count(case when title_unique_links_count > 0 and title_link_type = 'link' then sl.id else 0 end ) as total_title_link_submissions,
		count(case when title_unique_links_count > 0 and title_link_type = 'mention' then sl.id else 0 end ) as total_title_mention_submissions,
		count(case when selftext_unique_links_count > 0 and title_link_type = 'link' then sl.id else 0 end) as total_selftext_link_submissions,
		count(case when selftext_unique_links_count > 0 and title_link_type = 'mention' then sl.id else 0 end) as total_selftext_mention_submissions,	
		
		sum(case when title_unique_links_count > 0 then 1 else 0 end ) as total_title_links_and_mentions,
		sum(case when selftext_unique_links_count > 0 then 1 else 0 end) as total_selftext_links_and_mentions,
	
		sum(case when title_unique_links_count > 0 and title_link_type = 'link' then 1 else 0 end ) as total_title_links,
		sum(case when title_unique_links_count > 0 and title_link_type = 'mention' then 1 else 0 end ) as total_title_mentions,
		sum(case when selftext_unique_links_count > 0 and title_link_type = 'link' then 1 else 0 end) as total_selftext_links,
		sum(case when selftext_unique_links_count > 0 and title_link_type = 'mention' then 1 else 0 end) as total_selftext_mentions
	
		
		
	from sub_months sm
	left join submission_links sl on sm.subreddit = sl.mentioned_sub_Name and sm.creation_delta_months = sl.creation_delta_months
	group by sm.subreddit, sm.creation_delta_months
),
cross_posts as (
	select
		sm.subreddit,
		sm.creation_delta_months,
		coalesce(cps.num_authors, 0) as num_crosspost_authors,
		coalesce(cps.non_deleted_authors, 0) as num_crosspost_nondeleted_authors,
		coalesce(cps.num_subreddits, 0) as num_crosspost_subreddits,
		coalesce(cps.total_cross_posts, 0) as num_crosspost_submissions
	from sub_months sm
	left join s2_cross_posts_30day_summary cps on cps.mentioned_sub_name = sm.subreddit and cps.creation_delta_months = sm.creation_delta_months
	
)
select
	sm.subreddit as mentioned_sub_name,
	sm.creation_delta_months,


	coalesce(ca.num_authors, 0) as num_comment_adv_authors,
	coalesce(ca.non_deleted_authors, 0) as num_comment_adv_non_deleted_authors,
	coalesce(ca.num_subreddits, 0) as num_comment_adv_subreddits,
	coalesce(ca.total_links_or_mentions, 0) as total_comment_adv_links_or_mentions,
	coalesce(ca.total_comment_adv_comments, 0) as total_comment_adv_comments,
	
	coalesce(ca.link_comments, 0) as comment_adv_link_comments,
	coalesce(ca.mention_comments, 0) as comment_adv_mention_comments,
		
	coalesce(ca.link_subreddits, 0) as comment_adv_link_subreddits,
	coalesce(ca.mention_subreddits, 0) as comment_adv_mention_subreddits,
	
	coalesce(ca.link_authors, 0) as comment_adv_link_authors,
	coalesce(ca.mention_authors, 0) as comment_adv_mention_authors,
	
	coalesce(ca.total_links, 0) as comment_adv_total_links,
	coalesce(ca.total_mentions, 0) as comment_adv_total_mentions,


	

	coalesce(ss.total_subreddits, 0) as num_submission_adv_subreddits,
	coalesce(ss.total_authors, 0) as num_submission_adv_authors,
	coalesce(ss.total_submissions, 0) as total_submission_adv_submissions,
	coalesce(ss.non_deleted_authors, 0) as num_submission_adv_non_deleted_authors,
		
	coalesce(ss.distinct_title_submissions, 0) as submission_adv_distinct_title_submissions,
	coalesce(ss.distinct_selftext_submissions, 0) as submission_adv_distinct_selftext_submissions,
			
	coalesce(ss.distinct_title_subreddits, 0) as submission_adv_distinct_title_subreddits,
	coalesce(ss.distinct_selftext_subreddits, 0) as submission_adv_distinct_selftext_subreddits,
		
	coalesce(ss.distinct_title_authors, 0) as submission_adv_distinct_title_authors,
	coalesce(ss.distinct_selftext_authors, 0) as submission_adv_distinct_selftext_authors,
		
	coalesce(ss.total_title_link_submissions, 0) as total_submission_adv_title_link_submissions,
	coalesce(ss.total_title_mention_submissions, 0) as total_submission_adv_title_mention_submissions,
	coalesce(ss.total_selftext_link_submissions, 0) as total_submission_adv_selftext_link_submissions,
	coalesce(ss.total_selftext_mention_submissions, 0) as total_submission_adv_selftext_mention_submissions,	
			
	coalesce(ss.total_title_links_and_mentions, 0) as total_submission_adv_title_links_and_mentions,
	coalesce(ss.total_selftext_links_and_mentions, 0) as total_submission_adv_selftext_links_and_mentions,
		
	coalesce(ss.total_title_links, 0) as total_submission_adv_title_links,
	coalesce(ss.total_title_mentions, 0) as total_submission_adv_title_mentions,
	coalesce(ss.total_selftext_links, 0) as total_submission_adv_selftext_links,
	coalesce(ss.total_selftext_mentions, 0) as total_submission_adv_selftext_mentions,

	coalesce(cp.num_crosspost_authors, 0) as num_crosspost_authors,
	coalesce(cp.num_crosspost_nondeleted_authors, 0) as num_crosspost_nondeleted_authors,
	coalesce(cp.num_crosspost_subreddits, 0) as num_crosspost_subreddits,
	coalesce(cp.num_crosspost_submissions, 0) as num_crosspost_submissions
	

into s2_subreddit_30day_inbound_advertising_data
from sub_months sm 
left join comment_ads ca on ca.mentioned_sub_name = sm.subreddit and ca.creation_delta_months = sm.creation_delta_months
left join submissions_summarized ss on ss.subreddit = sm.subreddit and ss.creation_delta_months = sm.creation_delta_months
left join cross_posts cp on cp.subreddit = sm.subreddit and cp.creation_delta_months = sm.creation_delta_months;




grant select on s2_subreddit_30day_inbound_advertising_data to public;

create index on s2_subreddit_30day_inbound_advertising_data(creation_delta_months);
create index on s2_subreddit_30day_inbound_advertising_data(mentioned_sub_name);



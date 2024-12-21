----------------------------------------
--
-- s3_inbound_subreddit_30day_inbound_advertising_data
--
----------------------------------------



drop table if exists s3_inbound_subreddit_30day_inbound_advertising_data;

with year_subs as (
	select 
		display_name,
		created_utc, 
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
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
submissions_summarized as (
	select
		ys.display_name as subreddit,
		(extract(epoch from (ascc.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,

		
		count(distinct ascc.subreddit) as total_subreddits,
		count(distinct ascc.id) as total_submissions,
		count(distinct ascc.author) as total_authors,
		count(distinct case when author != '[deleted]' then author else NULL end) as non_deleted_authors,
	
		count(distinct case when num_title_links+num_title_mentions > 0 then ascc.id else NULL end) as distinct_title_submissions,
		count(distinct case when num_selftext_mentions+num_selftext_links > 0 then ascc.id else NULL end) as distinct_selftext_submissions,
		
		count(distinct case when num_title_links+num_title_mentions > 0 then ascc.subreddit else NULL end) as distinct_title_subreddits,
		count(distinct case when num_selftext_mentions+num_selftext_links > 0 then ascc.subreddit else NULL end) as distinct_selftext_subreddits,
	
		count(distinct case when num_title_links+num_title_mentions > 0 then author else NULL end) as distinct_title_authors,
		count(distinct case when num_selftext_mentions+num_selftext_links > 0 then author else NULL end) as distinct_selftext_authors,
	
		count(case when num_title_links > 0 then ascc.id else 0 end ) as total_title_link_submissions,
		count(case when num_title_mentions > 0 then ascc.id else 0 end ) as total_title_mention_submissions,
		count(case when num_selftext_links > 0 then ascc.id else 0 end) as total_selftext_link_submissions,
		count(case when num_selftext_mentions > 0 then ascc.id else 0 end) as total_selftext_mention_submissions,	
		
		sum(num_title_links+num_title_mentions) as total_title_links_and_mentions,
		sum(num_selftext_mentions+num_selftext_links) as total_selftext_links_and_mentions,
	
		sum(num_title_links) as total_title_links,
		sum(num_title_mentions) as total_title_mentions,
		sum(num_selftext_links) as total_selftext_links,
		sum(num_selftext_mentions) as total_selftext_mentions,
		sum(num_cross_post) as total_cross_posts
	
		
		
	from year_subs ys
	left join s3_adv_submissions_counts_combined ascc on lower(ascc.mentioned_subreddit) = lower(ys.display_name)
	group by ys.display_name, creation_delta_months
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

	coalesce(ss.total_cross_posts, 0) as total_cross_posts
	
	

into s3_inbound_subreddit_30day_inbound_advertising_data
from sub_months sm 
left join comment_ads ca on ca.mentioned_sub_name = sm.subreddit and ca.creation_delta_months = sm.creation_delta_months
left join submissions_summarized ss on ss.subreddit = sm.subreddit and ss.creation_delta_months = sm.creation_delta_months;


grant select on s3_inbound_subreddit_30day_inbound_advertising_data to public;

create index on s3_inbound_subreddit_30day_inbound_advertising_data(creation_delta_months);
create index on s3_inbound_subreddit_30day_inbound_advertising_data(mentioned_sub_name);



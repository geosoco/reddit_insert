----------------------------------------
--
-- subreddit_inbound_advertising_data
--
----------------------------------------



drop table if exists subreddit_inbound_advertising_data;

with subreddit_list as (
	select ss.name, s.created_utc
	from subreddit_summary ss
	left join subreddits s on s.display_name = ss.name
	where total_activity >= 1000 and unique_authors >= 10
),
comment_ads as (
	select
		sl.name as mentioned_sub_name,
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

	
		
	from subreddit_list sl
	left join s2_comment_sub_link_details sld on sld.mentioned_sub_name = lower(sl.name)
	where self_reference != TRUE 
	group by sl.name
),
submission_links as (
select
	a.id,
	a.mentioned_sub_name,
	coalesce(stsld.subreddit, sssld.subreddit) as subreddit,
	coalesce(stsld.created_utc, sssld.created_utc) as created_utc,
	coalesce(stsld.author, sssld.author) as author,

	coalesce(stsld.unique_links_count,0) as title_unique_links_count,
	coalesce(sssld.unique_links_count,0) as selftext_unique_links_count,

	stsld.link_type as title_link_type,
	sssld.link_type as selftext_link_type
	
	from subreddit_list sl
	left join ( 
		select
			distinct stsld.id, sl.name as mentioned_sub_name, stsld.created_utc
		from subreddit_list sl
		left join s2_submission_title_sub_link_details stsld on lower(sl.name) = stsld.mentioned_sub_name
		union all
		select
			distinct sssld.id, sl.name as mentioned_sub_name, sssld.created_utc
		from subreddit_list sl
		left join s2_submission_selftext_sub_link_details sssld on lower(sl.name) = sssld.mentioned_sub_name
	) a	on a.mentioned_sub_name = sl.name
	left join s2_submission_title_sub_link_details stsld on stsld.id = a.id
	left join s2_submission_selftext_sub_link_details sssld on sssld.id = a.id
),
submissions_summarized as (
	select
		s.name as subreddit,

		count(distinct sl.subreddit) as total_subreddits,
		count(distinct sl.id) as total_submissions,
		count(distinct sl.author) as total_authors,
		count(distinct case when author != '[deleted]' then author else NULL end) as non_deleted_authors,
	
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
	
		
		
	from subreddit_list s
	left join submission_links sl on s.name = sl.mentioned_sub_name
	group by s.name
)
select
	s.name as mentioned_sub_name,


	ca.num_authors as num_comment_adv_authors,
	ca.non_deleted_authors as num_comment_adv_non_deleted_authors,
	ca.num_subreddits as num_comment_adv_subreddits,
	ca.total_links_or_mentions as total_comment_adv_links_or_mentions,
	ca.total_comment_adv_comments as total_comment_adv_comments,
	
	ca.link_comments as comment_adv_link_comments,
	ca.mention_comments as comment_adv_mention_comments,
		
	ca.link_subreddits as comment_adv_link_subreddits,
	ca.mention_subreddits as comment_adv_mention_subreddits,
	
	ca.link_authors as comment_adv_link_authors,
	ca.mention_authors as comment_adv_mention_authors,
	
	ca.total_links as comment_adv_total_links,
	ca.total_mentions as comment_adv_total_mentions,


	

	ss.total_subreddits as num_submission_adv_subreddits,
	ss.total_authors as num_submission_adv_authors,
	ss.total_submissions as total_submission_adv_submissions,
	ss.non_deleted_authors as num_submission_adv_non_deleted_authors,
		
	ss.distinct_title_submissions as submission_adv_distinct_title_submissions,
	ss.distinct_selftext_submissions as submission_adv_distinct_selftext_submissions,
			
	ss.distinct_title_subreddits as submission_adv_distinct_title_subreddits,
	ss.distinct_selftext_subreddits as submission_adv_distinct_selftext_subreddits,
		
	ss.distinct_title_authors as submission_adv_distinct_title_authors,
	ss.distinct_selftext_authors as submission_adv_distinct_selftext_authors,
		
	ss.total_title_link_submissions as total_submission_adv_title_link_submissions,
	ss.total_title_mention_submissions as total_submission_adv_title_mention_submissions,
	ss.total_selftext_link_submissions as total_submission_adv_selftext_link_submissions,
	ss.total_selftext_mention_submissions as total_submission_adv_selftext_mention_submissions,	
			
	ss.total_title_links_and_mentions as total_submission_adv_title_links_and_mentions,
	ss.total_selftext_links_and_mentions as total_submission_adv_selftext_links_and_mentions,
		
	ss.total_title_links as total_submission_adv_title_links,
	ss.total_title_mentions as total_submission_adv_title_mentions,
	ss.total_selftext_links as total_submission_adv_selftext_links,
	ss.total_selftext_mentions as total_submission_adv_selftext_mentions
	

into subreddit_inbound_advertising_data
from subreddit_list s
left join comment_ads ca on ca.mentioned_sub_name = s.name
left join submissions_summarized ss on ss.subreddit = s.name;




grant select on subreddit_inbound_advertising_data to public;

create index on subreddit_inbound_advertising_data(mentioned_sub_name);



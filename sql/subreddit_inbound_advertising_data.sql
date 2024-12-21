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
	where total_activity >= 400 and unique_authors >= 10
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
submissions_summarized as (
	select
		s.name as subreddit,

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
	
		
		
	from subreddit_list s
	left join s3_adv_submissions_counts_combined ascc on ascc.mentioned_subreddit = s.name
	group by s.name
)
select
	s.name as mentioned_sub_name,


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
	

into subreddit_inbound_advertising_data
from subreddit_list s
left join comment_ads ca on ca.mentioned_sub_name = s.name
left join submissions_summarized ss on ss.subreddit = s.name;




grant select on subreddit_inbound_advertising_data to public;

create index on subreddit_inbound_advertising_data(mentioned_sub_name);



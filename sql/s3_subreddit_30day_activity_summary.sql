drop table if exists s3_subreddit_30day_activity_summary;

with sub_moderators as (
	select distinct subreddit, moderator
	from subreddit_moderator_updates
),
sub_creators as (
	select distinct subreddit, creator
	from subreddit_creator_updates	
),
subreddit_creation as (
	select display_name, created_utc
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
),
lead_data as (
	select
		author,
		usa.subreddit, 
		creation_delta_months,
		lead(creation_delta_months) over (partition by usa.subreddit, author order by creation_delta_months asc) as next_active_month,
		case when lead(creation_delta_months) over (partition by usa.subreddit, author order by creation_delta_months asc) = creation_delta_months+1 then 1 else 0 end as active_next_month,
		case when sm.moderator is not null then 1 else 0 end as  is_mod,
		case when sc2.creator is not null then 1 else 0 end as  is_creator,
		total_activity,
		total_submissions,
		total_comments
	from subreddit_creation sc
	left join user_sub_activity_30day_activity usa on usa.subreddit = sc.display_name
	left join sub_moderators sm on sm.subreddit = sc.display_name and sm.moderator = usa.author
	left join sub_creators sc2 on sc2.subreddit = sc.display_name and sc2.creator = usa.author
),
sub_activity as (
	select
		subreddit, creation_delta_months, count(author) as unique_authors, 
		sum(total_activity) as total_activity,
		sum(total_comments) as total_comments,
		sum(total_submissions) as total_submissions,

		sum(case when total_comments > 0 and total_submissions > 0 then 1 else 0 end) as sub_and_comment_authors,
		sum(case when total_activity = total_comments then 1 else 0 end) as comment_only_authors,
		sum(case when total_activity = total_submissions then 1 else 0 end) as submission_only_authors,
		sum(case when total_submissions > 0 then 1 else 0 end) as total_submitters,
		sum(case when total_comments > 0 then 1 else 0 end) as total_commenters,
	
	
		sum(active_next_month) as retentive_authors,
		sum(case when active_next_month = 1 then total_activity else 0 end) as retentive_activity,
		sum(case when active_next_month = 1 then 0 else total_activity end) as non_retentive_activity,
		sum(case when active_next_month = 1 then total_comments else 0 end) as retentive_comments,
		sum(case when active_next_month = 1 then total_submissions else 0 end) as retentive_submissions,
		sum(case when active_next_month = 1 and total_comments > 0 then 1 else 0 end) as retentive_commenters,
		sum(case when active_next_month = 1 and total_submissions> 0 then 1 else 0 end) as retentive_submitters,


		sum(case when author = '[deleted]' then 1 else 0 end) as has_deleted_author,
		sum(case when author = '[deleted]' then total_activity else NULL end) as deleted_activity,
		sum(case when author = '[deleted]' then total_submissions else NULL end) as deleted_submissions,
		sum(case when author = '[deleted]' then total_comments else NULL end) as deleted_comments

	from lead_data ld
	group by subreddit, creation_delta_months
)
select *
into s3_subreddit_30day_activity_summary
from sub_activity;


grant select on s3_subreddit_30day_activity_summary to public;

create index on s3_subreddit_30day_activity_summary(subreddit);



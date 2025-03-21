--
--
-- misc queries from 1/15
--
-- includes a histogram of past history prior to joining, a discovery of trump's AMA, and a debugging query for promotions
--




-- generate a histogram of each accounts activity prior to joining
--
-- NOTE: This query takes about 2 hours to run for just a single day of activity, and this includes
-- ignoring [deleted] and 'AutoModerator'
--


with year_subs as (
	select 
		display_name,
		created_utc
	
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
), political_authors as (
	select
		usa.subreddit as political_sub_first_content,
		usa.author,
		first_activity_time as sub_first_activity_time
	
	from year_subs ys 
	left join user_subreddit_activity usa on usa.subreddit = ys.display_name
	where usa.author not in ('[deleted]', 'AutoModerator')
), author_activity as (
	select 
		pa.*,

		row_number() over (partition by pa.sub_first_activity_time, pa.author order by uca.created_utc desc) as content_number
	from political_authors pa
	left join user_combined_activity uca on uca.author = pa.author 
	where uca.created_utc <= pa.sub_first_activity_time and uca.created_utc >= (pa.sub_first_activity_time - interval '1 day')
), aggregated_activity as (
	select
		political_sub_first_content, author, count(*) as total_activity
	from author_activity
	group by political_sub_first_content, author
)
select
	political_sub_first_content, total_activity, count(*) as cnt
from aggregated_activity
group by political_sub_first_content, total_activity
order by political_sub_first_content, cnt desc




--
--
-- Looks at some activity around a specific point
--
-- This was used to explore the source of some traffic, which turned out to be trump's AMA
--




with donald_joins as (
select
	subreddit,
	author, 
	first_activity_time,
	last_activity_time,
	total_activity,
	first_fostering_month,
	floor(extract('epoch' from uj.last_activity_time - uj.first_activity_time)/(3600*24)) as num_days_in_sub
from s3_user_joins_with_fostering uj
where subreddit = 'The_Donald' and first_activity_time >= '2016-07-26' and first_activity_time < '2016-07-30'
),
article_ids as (
select
	dj.*,
	first_comment_id,
	first_comment_time,
	first_submission_id,
	first_submission_time,
	coalesce(c.article, first_submission_id) as first_article_id
from donald_joins dj
left join user_subreddit_activity usa on usa.subreddit = dj.subreddit and usa.author = dj.author
left join comments_y2016_m07 c on c.id = first_comment_id
),
article_participation_counts as (
select
	first_article_id, count(*) as cnt,
	count(*) filter (where first_comment_time >= '2016-07-26' and first_comment_time < '2016-07-27'),
	count(*) filter (where first_comment_time >= '2016-07-27' and first_comment_time < '2016-07-28'),
	count(*) filter (where first_comment_time >= '2016-07-28' and first_comment_time < '2016-07-29'),
	count(*) filter (where first_comment_time >= '2016-07-29' and first_comment_time < '2016-07-30')
from article_ids
group by first_article_id
order by cnt desc
)
select 
	apc.*,
	data->>'title', 
	data->>'url',
	(data->>'score')::int,
	(data->>'num_comments')::int
from article_participation_counts apc
left join submissions s on s.id = apc.first_article_id







--
--
-- aggregate the number of mentions/links and their types to r/TD mostly for debugging purposes
--
--


with unique_items as (
select
	comment_id,
	submission_id,
	count(*) as cnt,
	array_agg(mentioned_sub_name) as mentioned_sub_names,
 	array_agg(link_type) as link_types,
	array_agg(mentioned_sub_link) as sub_links,
	max(
		case when link_type = 'crosspost' then 3 
		when link_type = 'link' then 2
		when link_type = 'mention' then 1
		else null end
	) as priority_type
	
from s3_inbound_mentions_combined_flat imcf
group by 
	comment_id,
	submission_id

)


select
 *
 
from unique_items ui
--left join s3_inbound_mentions_combined_flat imcf on imcf.comment_id = ui.comment_id and imcf.submission_id = ui.submission_id
where ui.cnt > 1 and submission_id is not null

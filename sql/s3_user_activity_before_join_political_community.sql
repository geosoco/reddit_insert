--
--
-- s3_user_activity_before_join_political_community
--
-- Builds a table of activity prior to joining one of the political subreddits
-- Note, because of overlap, authors can appear in multiple political subs
-- political_sub_first_content is the first piece of content within that subreddit
--
-- WARNING: This query takes about 7-8 hours to complete because it's contains high activity
-- accounts like AutoModerator
--


with year_subs as (
	select 
		display_name,
		created_utc, 
		extract(year from age('2017-01-01'::timestamp, created_utc)) * 12 + extract(month from age('2017-01-01'::timestamp, created_utc)) as max_cal_months
	
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
), political_authors as (
	select
		usa.subreddit as political_sub_first_content,
		usa.author,
		usa.first_activity_time as sub_first_activity_time,
		usa.total_activity as sub_total_activity,
		case when first_comment_time = first_activity_time then 'c' else 's' end as sub_first_activity_type
	
	from year_subs ys 
	left join user_subreddit_activity usa on usa.subreddit = ys.display_name
), author_activity as (
	select 
		pa.*,
		uca.c_id,
		uca.s_id,
		uca.created_utc,
		uca.subreddit,
		uca.score,
		uca.length,
		uca.deleted,
		uca.removed,
		extract(epoch from (pa.sub_first_activity_time - uca.created_utc)) as delta_activity_time,
		row_number() over (partition by pa.author order by uca.created_utc desc) as content_number
	from political_authors pa
	left join user_combined_activity uca on uca.author = pa.author
	where uca.created_utc <= pa.sub_first_activity_time
)
select * 
into s3_user_activity_before_join_political_community
from author_activity
where content_number <= 1000;


grant select on s3_user_activity_before_join_political_community to public;

create index on s3_user_activity_before_join_political_community(author);
create index on s3_user_activity_before_join_political_community(subreddit);
create index on s3_user_activity_before_join_political_community(content_number);

-----------------------------------------------------------
--
-- Misc queries 2024-08-14
--
-----------------------------------------------------------





-----------------------------------------------------------
--
--
--
-----------------------------------------------------------


with fostering_authors as (
	select
		susd.subreddit, 
		susd.author, 
		min(susd.first_delta_month) as first_fostering_month

		
		
	from s3_sub_user_monthly_sequence_data susd
	where susd.total_months >= 4 and susd.total_submissions > (total_months * 20) 
	
	group by susd.subreddit, susd.author
),
fostered_submissions as (
	select
		fa.subreddit,
		fa.author,
		min(first_fostering_month),
		age(date_trunc('month', s.created_utc), first_fostering_month) as  rel_month,
		count(*) as total_submissions,
		count(distinct s.domain) as total_domains,
		(count(*))::decimal / count(distinct s.domain) as submissions_to_domains
	from fostering_authors fa
	left join s3_political_submissions s on s.subreddit = fa.subreddit and s.author = fa.author
	group by fa.subreddit, fa.author, age(date_trunc('month', s.created_utc), first_fostering_month)
)
select 
	*
from fostered_submissions




-----------------------------------------------------------
--
-- Possible query for coding
--
-----------------------------------------------------------


select setseed(-0.314);

with 
segmented_text_posts as (
	select 
		case 
		when created_utc < '2015-07-01' then -1
		when created_utc >= '2015-07-01' and created_utc < '2016-02-01' then 0
		when created_utc >= '2016-02-01' and created_utc < '2016-08-01' then 1
		else 2 end as period,
		*
	from s3_political_submissions
	where has_removed_text = false	and is_text_post = true
),
random_text_posts as (
	select 
		row_number() over( partition by subreddit, period order by random()) as rownum,
		id, id36, subreddit, created_utc, period, author, 
		'' as c1,
		'' as c2,
		'' as c3,
		'' as c4,
		title, 
		selftext,
		('https://reddit.com' || (data->>'permalink')::char ) as link,
		score, num_comments_from_data
	from segmented_text_posts
	where period >= 0
)
select 
	*
from random_text_posts where rownum <= 200;















-----------------------------------------------------------
--
--
--
-----------------------------------------------------------





select
	case when has_deleted_text = true then 'deleted'
	when has_removed_text = true then 'removed'
	when is_text_post = true then 'text'
	else 'other' end as post_type, 
	count(*)
from s3_political_submissions
where subreddit = 'SandersForPresident'
group by post_type







-----------------------------------------------------------
--
--
--
-----------------------------------------------------------


with 
short_links_raw as (
select
		subreddit, id, created_utc, author, title, score, num_comments_from_data, domain, url, 
		(regexp_match(url, '(?<!//i\.)redd.it/([\w\d]{5,6})$'))[1] as source_id36
	from s3_political_submissions
where domain ~* '^(redd\.it|np\.redd\.it)' 
),
short_links as (
select 
	slr.*,
	case when source_id36 is not null and length(source_id36) > 4 then base36_decode(source_id36) else null end as source_id
	from short_links_raw slr
	where source_id36 is not null and length(source_id36) > 4
),
cross_links as (
	select subreddit, created_utc, date_trunc('month', created_utc) as month, domain, url,
		case when domain = 'self.' || subreddit then 'self' 
		when 

	from s3_political_submissions
	where domain ~* '^(np\.)?reddit.com'
)

select
sl.*, s.subreddit as source_subreddit, s.author as source_author, s.created_utc as source_created_utc, s.data->>'score' as source_score
from short_links sl
left join submissions s on s.id = sl.source_id
--limit 100








-----------------------------------------------------------
--
--
--
-----------------------------------------------------------




with link_type as (
	select subreddit, created_utc, date_trunc('month', created_utc) as month, domain, url,
		case when domain = 'self.' || subreddit then 'self' 
		when domain ~* '^(np\.)?reddit.com' or domain ~* '^redd.it' then 'xpost'
		else 'ext' end as link_type
	from s3_political_submissions
)

select subreddit, link_type, month, count(*)
from link_type
group by subreddit, link_type, month
order by month, subreddit,  link_type












-----------------------------------------------------------
--
--
--
-----------------------------------------------------------



with link_type as (
	select subreddit, created_utc, date_trunc('month', created_utc) as month, domain, url,
		case when domain = 'self.' || subreddit then 'self' 
		when domain ~* '^(np\.)?reddit.com' or domain ~* '^redd.it' then 'xpost'
		else 'ext' end as link_type
	from s3_political_submissions
)

select subreddit, link_type, month, count(*)
from link_type
group by subreddit, link_type, month
order by month, subreddit,  link_type











-----------------------------------------------------------
--
--
--
-----------------------------------------------------------


with link_type as (
	select subreddit, created_utc, date_trunc('month', created_utc) as month, domain, url,
		case when domain = 'self.' || subreddit then 'self' 
		when domain ~* '^(np\.)?reddit.com' or domain ~* '^redd.it' then 'xpost'
		else 'ext' end as link_type
	from s3_political_submissions
)

select subreddit, link_type, month, count(*)
from link_type
group by subreddit, link_type, month
order by month, subreddit,  link_type










-----------------------------------------------------------
--
--
--
-----------------------------------------------------------


with sub_moderators as (
	select distinct subreddit, moderator
	from s3_moderator_updates
),
creators as (
	select distinct subreddit, creator
	from s3_creator_updates
	where creator != '[deleted]'
),
	
fostering_data as (
	select
		s.subreddit, 
		s.month_year,
		
		count(distinct susd.author) as num_fostering_authors,
		sum(s.total_activity) as total_fostering_activity,
		sum(s.total_submissions) as total_fostering_submissions,
		sum(s.total_comments) as total_fostering_comments,
		sum(case when m.moderator is not null then 1 else 0 end) as num_fostering_mods,
		sum(case when m.moderator is not null then s.total_activity else 0 end) as total_moderator_fostering_activity,
		sum(case when m.moderator is not null then s.total_submissions else 0 end) as total_moderator_fostering_submissions,
		sum(case when m.moderator is not null then s.total_comments else 0 end) as total_moderator_fostering_comments,
		sum(case when c.creator is not null then 1 else 0 end) as num_fostering_creators,
		sum(case when c.creator is not null then s.total_activity else 0 end) as total_creator_fostering_activity,
		sum(case when c.creator is not null then s.total_submissions else 0 end) as total_creator_fostering_submissions,
		sum(case when c.creator is not null then s.total_comments else 0 end) as total_creator_fostering_comments
		
	from s3_user_sub_activity_monthly_activity s
	left join s3_sub_user_monthly_sequence_data susd on s.subreddit = susd.subreddit and s.author = susd.author
	left join creators c on c.subreddit = s.subreddit and c.creator = s.author
	left join sub_moderators m on m.subreddit = s.subreddit and m.moderator = s.author
	where susd.total_months >= 4  
	and susd.first_delta_month <= s.month_year and susd.last_delta_month >= s.month_year
	
	group by s.subreddit, s.month_year
)
select 
  fd.*,
  sas.total_activity,
  sas.total_comments,
  sas.total_submissions,
  sas.unique_authors
from fostering_data fd
left join s3_subreddit_monthly_activity sas on sas.subreddit = fd.subreddit and sas.month_year = fd.month_year






-----------------------------------------------------------
--
--
--
-----------------------------------------------------------


with moderators as (
select subreddit, moderator, min(added) as first_added, min(update_time) as min_update_time from s3_moderator_updates
group by subreddit, moderator
	having min(update_time) < '2017-01-01' 
	

),
moderator_content as (
	select
		m.*,
		usa.total_activity,
		usa.total_submissions,
		usa.total_comments
	from moderators m
	left join user_subreddit_activity usa on usa.subreddit = m.subreddit and usa.author = m.moderator
),
loyalty_data as (
	select
		mc.subreddit,
		us.author,
	
		us.total_activity as reddit_total_activity,
		us.total_submissions as reddit_total_submissions,
		us.total_comments as reddit_total_comments,

		case when us.total_activity > 0 then coalesce(mc.total_activity,0) * 100.0 / us.total_activity::decimal else 0 end as loyalty_activity_pct,
		case when us.total_submissions > 0 then coalesce(mc.total_submissions,0) * 100.0 / us.total_submissions::decimal else 0 end as loyalty_submissions_pct,
		case when us.total_comments > 0 then coalesce(mc.total_comments,0) * 100.0 / us.total_comments::decimal else 0 end as loyalty_comments_pct
		
	from moderator_content mc
	left join user_summary us on us.author = mc.moderator
),
sub_summary as (
	select
		mc.*,
		ss.total_activity as sub_total_activity,
		ss.total_submissions as sub_total_submissions,
		ss.total_comments as sub_total_comments,

		case when ss.total_activity > 0 then coalesce(mc.total_activity,0) * 100.0 / ss.total_activity::decimal else 0 end as sub_activity_pct,
		case when ss.total_submissions > 0 then coalesce(mc.total_submissions,0) * 100.0 / ss.total_submissions::decimal else 0 end as sub_submissions_pct,
		case when ss.total_comments > 0 then coalesce(mc.total_comments,0) * 100.0 / ss.total_comments::decimal else 0 end as sub_comments_pct
		
		
	from moderator_content mc
	left join subreddit_summary ss on ss.name = mc.subreddit
	
)

select * 
from sub_summary ss
left join loyalty_data ld on ss.subreddit = ld.subreddit and ss.moderator = ld.author






-----------------------------------------------------------
--
--
--
-----------------------------------------------------------



with fostering_authors as (
	select
		susd.subreddit, 
		susd.author, 
		min(susd.first_delta_month) as first_fostering_month

		
		
	from s3_sub_user_monthly_sequence_data susd
	where susd.total_months >= 4 and susd.total_submissions > (total_months * 20) 
	
	group by susd.subreddit, susd.author
),
fostered_submissions as (
	select
		s.id as fostered_submission_id		
	from fostering_authors fa
	left join s3_political_submissions s on s.subreddit = fa.subreddit and s.author = fa.author
),
all_submissions as (
	select
		id,
		subreddit,
		case when fostered_submission_id is null then false else true end as is_fostered,
		fostered_submission_id,
		is_text_post
	from s3_political_submissions s
	left join fostered_submissions fs on fs.fostered_submission_id = s.id
),
combined as (
	select
		subreddit, is_fostered, is_text_post, count(*) as cnt
	from all_submissions
	group by subreddit, is_fostered, is_text_post
)
select
	c.*, cnt * 100.0 / (sum(cnt) over (partition by subreddit, is_fostered) )
from combined c

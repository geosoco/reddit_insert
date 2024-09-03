--
--
-- misc queries from 2024-06-21
--
--

with year_subs as (
	select 
		display_name,
		created_utc, 
		extract(year from age('2017-01-01'::timestamp, created_utc)) * 12 + extract(month from age('2017-01-01'::timestamp, created_utc)) as max_cal_months
	
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
),
sub_months as (
	select 
	ys.display_name as subreddit, 
	ys.max_cal_months,
	created_utc,
	--month as creation_delta_months,
	date_trunc('month', created_utc + (interval '1 month' * month)) as month_year


	from year_subs ys
	cross join lateral generate_series(0, ys.max_cal_months::int) m(month)
	order by ys.display_name, month
),
submission_data as (
	select
		ps.subreddit,
		ps.author,
		--(extract(epoch from (ps.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
		date_trunc('month', ps.created_utc) as month_year,
		sum(num_comments_from_data) as total_comments_from_submissions,
		sum(score) as total_score_from_submissions


	from s3_political_submissions ps
	group by ps.subreddit, date_trunc('month', ps.created_utc), ps.author
),
user_sub_monthly as (
	select
		sm.subreddit,
		sm.month_year,
		usa.author,

		usa.total_activity,
		usa.total_submissions,
		usa.total_comments,

		sd.total_comments_from_submissions,
		sd.total_score_from_submissions
		
	from sub_months sm
	left join s3_user_sub_activity_monthly_activity usa on usa.subreddit = sm.subreddit and usa.month_year = sm.month_year
	left join submission_data sd on sd.subreddit = sm.subreddit and sd.month_year = sm.month_year and sd.author = usa.author
),
fostering_data as (
	select
		sm.subreddit,
		sm.month_year,
		
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then 1 else 0 end) as total_fosterers,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then usm.total_activity else 0 end) as total_activity,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then usm.total_submissions else 0 end) as total_submissions,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then usm.total_comments else 0 end) as total_comments,

		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then coalesce(total_comments_from_submissions,0) else 0 end) as total_comments_from_submissions,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then coalesce(total_score_from_submissions,0) else 0 end) as total_score_from_submissions
	
	from sub_months sm
	left join user_sub_monthly usm on sm.subreddit = usm.subreddit and sm.month_year = usm.month_year
	left join s3_sub_user_monthly_sequence_data susd on sm.subreddit = susd.subreddit and usm.author = susd.author

	where susd.total_months >= 4 and susd.total_activity > (total_months * 20) 
	group by sm.subreddit, sm.month_year

	order by sm.subreddit, month_year asc
),
subreddit_monthly_activity as (
	select
		sm.subreddit,
		sm.month_year,
	
		count(distinct author) as unique_authors,
		sum(total_activity) as total_activity,
		sum(total_comments) as total_comments,
		sum(total_submissions) as total_submissions,
		sum(total_score_from_submissions) as total_submission_score,
		sum(total_comments_from_submissions) as total_submission_comments
	
	from sub_months sm
	left join user_sub_monthly usa on usa.subreddit = sm.subreddit and usa.month_year = sm.month_year
	group by sm.subreddit, sm.month_year
)

select
	sm.*, 
	
	sma.unique_authors,
	sma.total_activity,
	sma.total_comments,
	sma.total_submissions,
	sma.total_submission_score,
	sma.total_submission_comments,
 
	
	case when sma.unique_authors > 0 then coalesce(total_fosterers,0) * 100.0 / sma.unique_authors else 0 end as pct_fostering_authors,
	case when sma.total_activity > 0 then coalesce(fd.total_activity,0) * 100.0 / sma.total_activity else 0 end as pct_fostering_total_activity,
	case when sma.total_submissions > 0 then coalesce(fd.total_submissions,0) * 100.0 / sma.total_submissions else 0 end as pct_fostering_total_submissions,
	case when sma.total_comments > 0 then coalesce(fd.total_comments,0) * 100.0 / sma.total_comments else 0 end as pct_fostering_total_comments,
	case when sma.total_submission_score > 0 then coalesce(fd.total_score_from_submissions,0) * 100.0 / sma.total_submission_score else 0 end as pct_total_fostering_submission_score,
	case when sma.total_submission_comments > 0 then coalesce(fd.total_comments_from_submissions,0) * 100.0 / sma.total_submission_comments else 0 end as pct_total_fostering_submission_score

from sub_months sm
left join fostering_data fd on fd.subreddit = sm.subreddit and sm.month_year = fd.month_year
left join subreddit_monthly_activity sma on sma.subreddit = sm.subreddit and sma.month_year = sm.month_year




--






with mods as (
	select
		subreddit, moderator
	from s3_moderator_updates mu 
	where subreddit = 'The_Donald'
	group by subreddit, moderator
)
select
	date_trunc('month', created_utc) as month,
	count(*) as total_submissions,
	sum(case when m.moderator is not null then 1 else 0 end) as count_mod_posts,
	sum(case when m.moderator = 'jcm267' then 1 else 0 end) as count_jcm,
	sum(case when m.moderator = 'NYPD-32' then 1 else 0 end) as count_nypd32,
	sum(case when m.moderator = 'Medically' then 1 else 0 end) as count_nmedically,
	sum(case when m.moderator = 'Phinaeus' then 1 else 0 end) as count_phinaeus,
	sum(case when m.moderator = 'WeWantTrump' then 1 else 0 end) as count_wewanttrump,
	array_agg(distinct m.moderator) filter (where m.moderator is not null) as mods,
	count(distinct m.moderator) as unique_mods
from  
	s3_political_submissions sps
	left join mods m on m.subreddit = sps.subreddit and m.moderator = sps.author
where sps.subreddit = 'The_Donald' and sps.created_utc < '2016-04-01'
group by date_trunc('month', created_utc)
order by month asc






with posts as (
select 
	case when has_deleted_text is true then 'deleted' 
	when has_removed_text is true then 'removed'
	when is_text_post is true then 'text'
	else 'link' end as post_type
from s3_political_submissions sps
where sps.subreddit = 'The_Donald' and created_utc between '2015-06-27' and ('2015-06-27'::date + interval '31 day')
)
select post_type, count(*)
	from posts
	group by post_type








with sub_data as (
	select display_name as subreddit, created_utc from subreddits where display_name = 'The_Donald'
)
select  sps.created_utc - sd.created_utc as rel_time, sps.* 
from sub_data sd
	left join s3_political_submissions sps on sd.subreddit = sps.subreddit
	order by created_utc asc limit 1000;








	select
		s.subreddit, 
		s.creation_delta_months,
		
		count(distinct susd.author) as num_fostering_authors,
		sum(s.total_activity) as total_fostering_activity,
		sum(s.total_submissions) as total_fostering_submissions,
		sum(s.total_comments) as total_fostering_comments,
		sum(susd.is_mod) as num_fostering_mods,
		sum(case when susd.is_mod = 1 then s.total_activity else 0 end) as total_moderator_fostering_activity,
		sum(case when susd.is_mod = 1 then s.total_submissions else 0 end) as total_moderator_fostering_submissions,
		sum(case when susd.is_mod = 1 then s.total_comments else 0 end) as total_moderator_fostering_comments
		
	from s3_sub_user_retention_intermediate s
	left join s3_sub_user_sequence_data susd on s.subreddit = susd.subreddit and s.author = susd.author

	where 
		s.subreddit = 'The_Donald'
	and susd.total_months >= 6 and susd.total_activity > (total_months * 30) 
	and susd.first_delta_month <= s.creation_delta_months and susd.last_delta_month >= s.creation_delta_months
	
	group by s.subreddit, s.creation_delta_months









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
	month as creation_delta_months,
	created_utc + interval '1 day' * (month * 30) as month_start_date,
	created_utc + interval '1 day' * (month * 30 + 29) as month_end_date

	from year_subs ys
	cross join lateral generate_series(0, floor(ys.max_months)::int) m(month)
	order by ys.display_name, month
),
fostering_data as (
	select
		sm.subreddit,
		sm.creation_delta_months,
		
		sum(case when susd.first_delta_month = sm.creation_delta_months then 1 else 0 end) as num_joins,
		sum(case when susd.last_delta_month = sm.creation_delta_months then 1 else 0 end) as num_exits,
		sum(case when susd.last_delta_month = sm.creation_delta_months then 1 else 0 end) as num_censored_exits,
		sum(case when susd.first_delta_month <= sm.creation_delta_months and susd.last_delta_month > sm.creation_delta_months then 1 else 0 end) as total_fosterers,
		sum(case when susd.first_delta_month <= sm.creation_delta_months and susd.last_delta_month > sm.creation_delta_months then usa.total_activity else 0 end) as total_activity,
		sum(case when susd.first_delta_month <= sm.creation_delta_months and susd.last_delta_month > sm.creation_delta_months then usa.total_submissions else 0 end) as total_submissions,
		sum(case when susd.first_delta_month <= sm.creation_delta_months and susd.last_delta_month > sm.creation_delta_months then usa.total_comments else 0 end) as total_comments
		
	from sub_months sm
	left join user_sub_activity_30day_activity usa on usa.subreddit = sm.subreddit and sm.creation_delta_months = usa.creation_delta_months
	left join s3_sub_user_sequence_data susd on sm.subreddit = susd.subreddit and usa.author = susd.author
	


	where susd.total_months >= 3 and susd.total_activity > (total_months * 20) 
	group by sm.subreddit, sm.creation_delta_months

	order by subreddit, creation_delta_months asc
)
select
	sm.*, 
	coalesce(fd.num_joins, 0) as num_joins, 
	coalesce(fd.num_exits, 0) as num_exits,
	coalesce(fd.total_fosterers, 0) as total_fosterers, 
	coalesce(fd.total_activity,0) as fostering_activity, 
	coalesce(fd.total_submissions,0) as fostering_submissions, 
	coalesce(fd.total_comments,0) as fostering_comments,
	coalesce(sas.unique_authors, 0) as unique_authors, 
	coalesce(sas.total_activity,0) as total_activity, 
	coalesce(sas.total_submissions, 0) as total_submissions,
	coalesce(sas.total_comments, 0) as total_comments,
	case when sas.unique_authors > 0 then total_fosterers * 100.0 / sas.unique_authors else 0 end as pct_fostering_authors,
	case when sas.total_activity > 0 then fd.total_activity * 100.0 / sas.total_activity else 0 end as pct_total_activity,
	case when sas.total_submissions > 0 then fd.total_submissions * 100.0 / sas.total_submissions else 0 end as pct_total_submissions,
	case when sas.total_comments > 0 then fd.total_comments * 100.0 / sas.total_comments else 0 end as pct_total_comments

from sub_months sm
left join fostering_data fd on fd.subreddit = sm.subreddit and sm.creation_delta_months = fd.creation_delta_months
left join s3_subreddit_30day_activity_summary sas on sas.subreddit = sm.subreddit and sas.creation_delta_months = sm.creation_delta_months












with year_subs as (
	select 
		display_name,
		created_utc, 
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months,
		extract(year from age('2017-01-01'::timestamp, created_utc)) * 12 + extract(month from age('2017-01-01'::timestamp, created_utc)) as max_cal_months
	
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
),
sub_months as (
	select 
	ys.display_name as subreddit, 
	ys.max_months,
	ys.max_cal_months,
	created_utc,
	max_months,
	--month as creation_delta_months,
	date_trunc('month', created_utc + (interval '1 month' * month)) as month_year


	from year_subs ys
	cross join lateral generate_series(0, ys.max_cal_months::int) m(month)
	order by ys.display_name, month
),
fostering_data as (
	select
		sm.subreddit,
		sm.month_year,
		
		sum(case when susd.first_delta_month = sm.month_year then 1 else 0 end) as num_joins,
		sum(case when susd.last_delta_month = sm.month_year then 1 else 0 end) as num_exits,
		sum(case when susd.last_delta_month = sm.month_year then 1 else 0 end) as num_censored_exits,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then 1 else 0 end) as total_fosterers,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then usa.total_activity else 0 end) as total_activity,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then usa.total_submissions else 0 end) as total_submissions,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then usa.total_comments else 0 end) as total_comments
		
	from sub_months sm
	left join s3_user_sub_activity_monthly_activity usa on usa.subreddit = sm.subreddit and sm.month_year = usa.month_year
	left join s3_sub_user_monthly_sequence_data susd on sm.subreddit = susd.subreddit and usa.author = susd.author
	


	where susd.total_months >= 3 and susd.total_activity > (total_months * 20) 
	group by sm.subreddit, sm.month_year

	order by subreddit, month_year asc
)
select
	sm.*, 
	coalesce(fd.num_joins, 0) as num_joins, 
	coalesce(fd.num_exits, 0) as num_exits,
	coalesce(fd.total_fosterers, 0) as total_fosterers, 
	coalesce(fd.total_activity,0) as fostering_activity, 
	coalesce(fd.total_submissions,0) as fostering_submissions, 
	coalesce(fd.total_comments,0) as fostering_comments,
	coalesce(sas.unique_authors, 0) as unique_authors, 
	coalesce(sas.total_activity,0) as total_activity, 
	coalesce(sas.total_submissions, 0) as total_submissions,
	coalesce(sas.total_comments, 0) as total_comments,
	case when sas.unique_authors > 0 then total_fosterers * 100.0 / sas.unique_authors else 0 end as pct_fostering_authors,
	case when sas.total_activity > 0 then fd.total_activity * 100.0 / sas.total_activity else 0 end as pct_total_activity,
	case when sas.total_submissions > 0 then fd.total_submissions * 100.0 / sas.total_submissions else 0 end as pct_total_submissions,
	case when sas.total_comments > 0 then fd.total_comments * 100.0 / sas.total_comments else 0 end as pct_total_comments

from sub_months sm
left join fostering_data fd on fd.subreddit = sm.subreddit and sm.month_year = fd.month_year
left join s3_subreddit_monthly_activity sas on sas.subreddit = sm.subreddit and sas.month_year = sm.month_year
	
	
	
	




with year_subs as (
	select 
		display_name,
		created_utc, 
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months,
		extract(year from age('2017-01-01'::timestamp, created_utc)) * 12 + extract(month from age('2017-01-01'::timestamp, created_utc)) as max_cal_months
	
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
),
sub_months as (
	select 
	ys.display_name as subreddit, 
	ys.max_months,
	ys.max_cal_months,
	created_utc,
	max_months,
	--month as creation_delta_months,
	date_trunc('month', created_utc + (interval '1 month' * month)) as month_year


	from year_subs ys
	cross join lateral generate_series(0, ys.max_cal_months::int) m(month)
	order by ys.display_name, month
),
fostering_data as (
	select
		sm.subreddit,
		sm.month_year,
		
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then 1 else 0 end) as total_fosterers,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then usa.total_activity else 0 end) as total_activity,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then usa.total_submissions else 0 end) as total_submissions,
		sum(case when susd.first_delta_month <= sm.month_year and susd.last_delta_month > sm.month_year then usa.total_comments else 0 end) as total_comments
		
	from sub_months sm
	left join s3_user_sub_activity_monthly_activity usa on usa.subreddit = sm.subreddit and sm.month_year = usa.month_year
	left join s3_sub_user_monthly_sequence_data susd on sm.subreddit = susd.subreddit and usa.author = susd.author
	


	where susd.total_months >= 3 and susd.total_activity > (total_months * 20) 
	group by sm.subreddit, sm.month_year
)

select subreddit, 

	sum(total_activity) / sum(total_fosterers) as avg_author_activity,
	sum(total_submissions) / sum(total_fosterers) as avg_author_submissions,
	sum(total_comments) / sum(total_fosterers) as avg_author_comments

from fostering_data
group by subreddit












with fostering_data as (
	select
		subreddit,
		author,
	
		count(distinct seq_id) as num_fostering_sessions,
		min(first_delta_month) as first_delta_month,
		max(last_delta_month) as last_delta_month,
		sum(total_months) as total_fostering_months,
		sum(total_activity) as total_fostering_activity,
		sum(total_submissions) as total_fostering_submissions,
		sum(total_comments) as total_fostering_comments

	from s3_sub_user_monthly_sequence_data susd 


	where subreddit = 'The_Donald' 
	and susd.total_months >= 3 and susd.total_activity > (susd.total_months * 20) 
	group by subreddit, author
),
moderators as (
	select distinct subreddit, moderator
	from s3_moderator_updates
	where subreddit = 'The_Donald'
),
td_activity as (
	select
		author, 
		first_activity_time as td_first_activity_time,
		last_activity_time as td_last_activity_time,
		comment_score_sum,
		comment_score_avg,
		submission_score_sum,
		submission_score_avg
	from user_subreddit_activity usa 
	where subreddit = 'The_Donald'
)
select 
	fd.*, 
	mu.moderator is not null as is_mod, 
	coalesce(fd.total_fostering_activity) / us.total_activity as pct_activity_in_sub,
	case when us.total_submissions > 0 then coalesce(fd.total_fostering_submissions) / us.total_submissions else 0 end as pct_submissions_in_sub,
	case when us.total_comments > 0 then coalesce(fd.total_fostering_comments) / us.total_comments else 0 end as pct_comments_in_sub,
	first_activity_time,
	last_activity_time,
	extract(epoch from (td_first_activity_time - first_activity_time)) / (3600*24) as delta_td_activity,
	td.td_first_activity_time,
	td.td_last_activity_time,
	td.comment_score_sum,
	td.comment_score_avg,
	td.submission_score_sum,
	td.submission_score_avg
from fostering_data fd
left join moderators mu on mu.moderator = fd.author
left join user_summary us on us.author = fd.author
left join td_activity td on td.author = fd.author
order by subreddit, total_fostering_submissions desc












with fostering_authors as (
	select
		subreddit, count(distinct author) as fostering_authors,
		sum(total_activity) as fostered_activity, sum(total_submissions) as fostered_submissions, sum(total_comments) as fostered_comments
	from sub_user_sequence_data
	where total_months >= 4 and total_activity >= total_months * 20
	group by subreddit
--order by fostering_authors desc
)
select
fa.*, 
	ss.unique_authors, ss.total_activity, ss.total_submissions, ss.total_comments, 
	fostering_authors*100.0/ss.unique_authors as pct_fosterers,
	fostered_activity*100.0/ss.total_activity as pct_fostered_activity,
	case when ss.total_submissions > 0 then fostered_submissions*100.0/ss.total_submissions else 0 end as pct_fostered_submissions,
	case when ss.total_comments > 0 then fostered_comments*100.0/ss.total_comments else 0 end as pct_fostered_comments
from fostering_authors fa
left join subreddit_summary ss on ss.name = fa.subreddit
order by fostering_authors desc







with fostering_data as (
	select
		subreddit,
		author,
	
		count(distinct seq_id) as num_fostering_sessions,
		min(first_delta_month) as first_delta_month,
		max(last_delta_month) as last_delta_month,
		sum(total_months) as total_fostering_months,
		sum(total_activity) as total_fostering_activity,
		sum(total_submissions) as total_fostering_submissions,
		sum(total_comments) as total_fostering_comments

	from s3_sub_user_monthly_sequence_data susd 


	where subreddit = 'The_Donald' 
	and susd.total_months >= 3 and susd.total_activity > (susd.total_months * 20) 
	group by subreddit, author
)

select 
	fd.*, 
	coalesce(fd.total_fostering_activity) / us.total_activity as pct_activity_in_sub,
	case when us.total_submissions > 0 then coalesce(fd.total_fostering_submissions) / us.total_submissions else 0 end as pct_submissions_in_sub,
	case when us.total_comments > 0 then coalesce(fd.total_fostering_comments) / us.total_comments else 0 end as pct_comments_in_sub,
	first_activity_time,
	last_activity_time
from fostering_data fd
left join moderators mu on mu.moderator = fd.author
left join user_summary us on us.author = fd.author
order by subreddit, total_fostering_submissions desc








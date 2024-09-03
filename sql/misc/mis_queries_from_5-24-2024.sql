---------------------------------------------------
--
-- Misc queries from 5/24/2024
--
---------------------------------------------------


with active_comment_first_months as (
  select s2.subreddit, 
    count(distinct creation_delta_months) as active_months
    from s2_subreddit_30day_activity_summary s2
    where s2.total_comments > 0 and s2.creation_delta_months < 12
    group by subreddit
),
eligible_subs as (
	select ss.name, ys.created_utc, total_activity, total_comments, total_submissions,
		case when total_submissions < 1000 then 0
		when total_submissions < 2627 then 1
		when total_submissions < 6443 then 2
		else 3
		end as submission_bin
	from subreddit_summary ss
	inner join active_comment_first_months acfm on ss.name  = acfm.subreddit
	where acfm.active_months >= 6
),
mods as (
	select subreddit, count(distinct moderator) as num_moderators
	from subreddit_moderator_updates
	group by subreddit
),
creators as (
	select subreddit, count(distinct creator) as num_creators
	from subreddit_creator_updates
	group by subreddit
)
select m.num_moderators is not null, submission_bin, count(*) as total_subs, avg(total_activity) as avg_activity, max(total_activity) as total_activity, min(total_activity) as min
 from eligible_subs es
 left join mods m on m.subreddit = es.name
 --left join creators c on c.subreddit = es.name
 group by m.num_moderators is not null, submission_bin;







 --
 --
 --

with year_subs as (
	select display_name, created_utc, subscribers
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
),
eligible_subs as (
	select ss.name, ys.created_utc, total_activity, total_comments, total_submissions,
		case when total_submissions < 1000 then 0
		when total_submissions < 2627 then 1
		when total_submissions < 6443 then 2
		else 3
		end as submission_bin
	from year_subs ys 
	inner join subreddit_summary ss on ss.name  = ys.display_name
	where total_submissions >= 400
)
select
	es.created_utc,
	s.*
	from eligible_subs as es
	left join s2_subreddit_monthly_data_combined s on es.name = s.subreddit
where 
	creation_delta_months = 0 and n2_present = TRUE and n6_present = TRUE



with year_subs as (
	select display_name, created_utc, subscribers
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
),
creators as (
	select distinct subreddit, creator
	from subreddit_creator_updates
),
eligible_subs as (
	select ss.name, ys.created_utc, total_activity, total_comments, total_submissions,
		case when total_submissions < 1000 then 0
		when total_submissions < 2627 then 1
		when total_submissions < 6443 then 2
		else 3
		end as submission_bin
	from year_subs ys 
	inner join subreddit_summary ss on ss.name  = ys.display_name
	where total_submissions >= 400
)
	select s2.subreddit, 
	count(distinct s2.author) as num_seed_authors, 
	sum(s2.total_submissions) num_seeded_content, 
	sum(s2.is_mod) num_seeding_mods, 
	sum(case when cr.creator is not null then 1 else 0 end) as creator_seeding
	from eligible_subs es
	left join s2_sub_user_retention_intermediate s2 on es.name = s2.subreddit
	left join creators cr on cr.subreddit = s2.subreddit and cr.creator = s2.author
	where creation_delta_months = 0 and s2.total_submissions > 10 and s2.author != '[deleted]' and s2.subreddit is null
	group by s2.subreddit
	order by s2.subreddit asc	








--
--
--


with base_subs as (
	select
		name, first_activity_time, first_comment_time
	from subreddit_summary ss
	where ss.first_comment_time >= '2013-01-01 00:00:00' and ss.first_comment_time < '2013-07-01 00:00:00'
	and total_comments >= 100
),
month_counts as (select
	bs.name, 
	count(distinct date_trunc('month', date)) as count_active_months
from base_subs bs
left join subreddit_summary_daily ssd on ssd.subreddit = bs.name
where comments_total_count is not null and comments_total_count > 0
group by bs.name
	)
	
select * from month_counts
where count_active_months > 1;


select * from subreddit








--
--
--


with year_subs as (
	select display_name, created_utc, subscribers
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
),
eligible_subs as (
	select ss.name, ys.created_utc, total_activity, total_comments, total_submissions,
		case when total_submissions < 1000 then 0
		when total_submissions < 2627 then 1
		when total_submissions < 6443 then 2
		else 3
		end as submission_bin
	from year_subs ys 
	inner join subreddit_summary ss on ss.name  = ys.display_name
	where total_submissions >= 400 
	-- and total_comments > 100
)

select
	count(distinct es.name) as num_subreddits,
	count(distinct case when usa.total_comments > 0 then es.name else NULL end) as subs_with_comments,
	sum(case when usa.total_submissions >= 20 then 1 else 0 end) as gte20,
	sum(case when usa.total_submissions >= 15 then 1 else 0 end) as gte15,
	sum(case when usa.total_submissions >= 10 then 1 else 0 end) as gte10,
	sum(case when usa.total_submissions >= 5 then 1 else 0 end) as gte5,
	count(distinct subreddit) as total_subreddits,
	count(distinct case when usa.total_submissions >= 10 then subreddit else NULL end) as subs_with_gte10,
	count(distinct case when usa.total_submissions >= 5 then subreddit else NULL end) as subs_with_gte5,
	avg(usa.total_submissions) as mean_submissions

from eligible_subs es 
left join user_sub_activity_30day_activity usa on usa.subreddit = es.name
where creation_delta_months = 0;







--
--
--



select *
	from s2_subreddit_30day_activity_summary where total_comments > 0 and total_submissions = 0 and creation_delta_months = 0
	

select
	usa.*
from user_sub_activity_30day_activity usa
where usa.total_comments > 0 and usa.total_submissions = 0 and creation_delta_months = 0







--
--
--




with 
tsugawa_subs as (
	select
		ss.name, ss.created_utc, ss.total_comments, ss.first_comment_time
	from
		subreddit_summary ss
	where ss.first_comment_time >= '2013-01-01 00:00:00' and ss.first_comment_time < '2013-07-01'
	
	--and 
	--and ss.created_utc >= '2013-01-01 00:00:00' and ss.created_utc < '2013-07-01 00:00:00'
	 
	and total_comments >= 100
),
-- comment_months as (
-- 	select 
-- 		ts.name as subreddit, creation_delta_months, sum(s30.total_comments) as sum_comments
-- 	from tsugawa_subs ts
-- 	left join user_sub_activity_30day_activity s30 on ts.name = s30.subreddit
-- 	group by ts.name, creation_delta_months
-- ),
-- eligible_subs as (
-- 	select 
-- 		cm.subreddit, count(distinct creation_delta_months) as num_active_comment_months
-- 	from comment_months cm
-- 	where sum_comments > 0
-- 	group by cm.subreddit
-- )
	
sub_monthly_engagement as (
	select 
		subreddit, date_trunc('month', date) as month, sum(total_count) as sum_comments
	from tsugawa_subs ts 
	left join subreddit_comments_summary_daily sc on sc.subreddit = ts.name
	group by subreddit, date_trunc('month', date)
),
eligible_subs as (
	select subreddit, count(distinct month) as num_active_comment_months
	from sub_monthly_engagement sme
	where sum_comments > 0
	group by subreddit
)

select * 
from tsugawa_subs ts
left join eligible_subs es on es.subreddit = ts.name
where es.num_active_comment_months > 1 






--
--
--

with subs as (
	select
		display_name, created_utc
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
)
select
	count(distinct s.display_name), 
	sum(case when ss.total_activity > 0 then 1 else 0 end) as has_activity, 
	sum(case when ss.total_activity > 10 then 1 else 0 end) as has_10_activity,
	sum(case when ss.total_comments > 100 then 1 else 0 end) as has_100_comments,
	sum(case when ss.total_comments > 100 and ss.unique_authors >= 10 then 1 else 0 end) as has_100_comments
from subs s
left join subreddit_summary ss on ss.name = s.display_name



select count(*) from submissions where created_utc >= '2012-01-01' and created_utc < '2013-01-01'



select * from subreddit_summary
where created_utc >= '2012-01-01' and created_utc < '2013-01-01' and total_comments >= 100 and unique_authors >= 20




--
--
--



with subreddit_ranges as (
	select
		name as subreddit,
		created_utc as created_utc,
		extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30) as max_months
	from subreddit_summary 
	where created_utc >= '2012-01-01' and created_utc <= '2013-01-01' and total_submissions >= 100 and unique_authors >= 10 and total_comments >= 10
),
sub_months as (
select
	r.subreddit,
	r.created_utc,
	floor(r.max_months) as max_month,
	month
--	creation_delta_months,
--	lag(creation_delta_months) over w as prev_month,
--	creation_delta_months - lag(creation_delta_months) over w as missing_months,
--	total_comments

from subreddit_ranges r
cross join lateral generate_series(0,floor(r.max_months)::int) m(month)
--full outer join s2_subreddit_30day_activity_summary ssa on a.subreddit = ssa.subreddit and ssa.creation_delta_months = m.month
--where creation_delta_months < 12 
--window w as (partition by a.subreddit order by creation_delta_months asc)
order by r.subreddit, month
)

select
	a.*,
	0+sum(missing_cmts_start) over w as seq_id
	from
	(
		select 
			sm.subreddit,
			sm.month,
			--sm.created_utc + interval '1 day' * (sm.month*30) as start_period,
			--sm.created_utc + interval '1 day' * ((sm.month+1)*30) as end_period,
			sm.max_month,
			creation_delta_months,
			
			lag(creation_delta_months) over w as prev_creation_delta_month,
			--0+sum(case when creation_delta_months is null and lag(creation_delta_months) over w = month - 1 then 1 else 0 end) over w as seq_id,
			case when creation_delta_months is null and ((lag(creation_delta_months) over w = month - 1) or month = 0) then 1 else 0 end as missing_start,
			case when 
				creation_delta_months is not null and lag(creation_delta_months) over w is null and month  > 0 
			then 1 else 0 end as missing_end,
			total_activity,
			total_comments,
			total_submissions,

			-- coalesce(total_comments,0) as ctc,
			-- coalesce(lag(total_comments) over w,0) as lctc,
			-- lag(total_comments) over w as ltc,

			-- case when month > 0 and total_comments is null then 1 else 0 end as test1,
			-- case when (total_comments is null or total_comments = 0) then 1 else 0 end as test2,
	
			case 
				when month > 0 
					and (  (total_comments is null or total_comments = 0 )
						and (lag(total_comments) over w) != 0)
				then 1 else 0 end as missing_cmts_start,

			case
				when month > 0
					and coalesce(total_comments,0) != 0
					and coalesce(lag(total_comments) over w, 0) = 0
				then 1 else 0 end as missing_cmts_end


		
		from sub_months sm
		left join s2_subreddit_30day_activity_summary ssa on sm.subreddit = ssa.subreddit and ssa.creation_delta_months = sm.month
		window w as (partition by sm.subreddit order by sm.month asc)
		order by sm.subreddit, sm.month	
	) a
	window w as (partition by a.subreddit order by a.month asc)



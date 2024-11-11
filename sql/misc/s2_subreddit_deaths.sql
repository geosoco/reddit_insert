-----------------------------------------
--
-- s2_subreddit_deaths
--
-----------------------------------------

drop table if exists s2_subreddit_deaths;

with subreddit_ranges as (
	select
		name as subreddit,
		ss.created_utc as created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddit_summary ss
	left join s2_subreddit_30day_activity_summary sas on sas.subreddit = ss.name 
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' 
--	and ss.total_submissions >= 100 and ss.unique_authors >= 10 and ss.total_comments >= 10
	and sas.creation_delta_months = 0 
	and sas.total_activity > 0
--	and name in ('210', '1000loantoday')
--	and name in ('2092', '20thcenturytrains', '210', '21conservative', '21crm', '2010sMusic', '200Situps', '1000loantoday' )
--and sas.total_comments is not null and sas.total_comments > 0
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
,
sub_month_borders as (
select
	a.*,
	0+sum(missing_comments_start) over w as seq_id
	from
	(
		select 
			sm.subreddit,
			sm.month,
			sm.created_utc + interval '1 day' * (sm.month*30) as start_period,
			sm.created_utc + interval '1 day' * ((sm.month+1)*30) as end_period,
			sm.max_month as max_month,
			creation_delta_months,

			
			
			lag(creation_delta_months) over w as prev_creation_delta_month,
			--0+sum(case when creation_delta_months is null and lag(creation_delta_months) over w = month - 1 then 1 else 0 end) over w as seq_id,
			case when creation_delta_months is null and (month = 0 or (lag(creation_delta_months) over w = month - 1)) then 1 else 0 end as missing_start,
			case when 
				creation_delta_months is not null and lag(creation_delta_months) over w is null and month  > 0 
			then 1 else 0 end as missing_end,
			total_activity,
			total_comments,
			total_submissions,
			lag(total_comments) over w as lagged_comments,

			-- coalesce(total_comments,0) as ctc,
			-- coalesce(lag(total_comments) over w,0) as lctc,
			-- lag(total_comments) over w as ltc,

			-- case when month > 0 and total_comments is null then 1 else 0 end as test1,
			-- case when (total_comments is null or total_comments = 0) then 1 else 0 end as test2,
	
			case 
				when 
					month > 0 and 
					(  coalesce(total_comments,0) = 0
						and ((lag(total_comments) over w) != 0))
				then 1 else 
					case when month = 0 and coalesce(total_comments,0) = 0 then 1 else 0 end
				end as missing_comments_start,

			case
				when month > 0
					and coalesce(total_comments,0) != 0
					and coalesce(lag(total_comments) over w, 0) = 0
				then 1 else 0 end as missing_comments_end


		
		from sub_months sm
		left join s2_subreddit_30day_activity_summary ssa on sm.subreddit = ssa.subreddit and ssa.creation_delta_months = sm.month
		window w as (partition by sm.subreddit order by sm.month asc)
		order by sm.subreddit, sm.month	
	) a
	window w as (partition by a.subreddit order by a.month asc)

)
--select * from sub_month_borders





,
missing_periods as (
	select
		smb.subreddit,
		smb.seq_id,
		max(smb.max_month) as max_month,
		min(start_period) as start_period_date,
		max(end_period) as end_period_date,
		min(month) as start_month,
		max(month) as end_month,
		(max(case when missing_comments_end = 1 then month else null end) - min(case when missing_comments_start = 1 then month else null end))::int as total_missing_months,
		min(case when missing_comments_start = 1 then month else null end) as missing_start,
		max(case when missing_comments_end = 1 then month else null end) as missing_end
--		max(case when missing_start = 1 then month else null end),
--		min(case when missing_start = 1 then month else null end),
--		array_agg(creation_delta_months) as months,
--		array_agg(missing_end),
--		array_agg(missing_start)
	from sub_month_borders smb
	group by smb.subreddit, smb.seq_id
)
--select * from missing_periods



,
fixed_missing_ends as (
	select
		subreddit,
		seq_id,
		max_month,
		start_period_date,
		end_period_date,
		start_month,
		missing_start,
		coalesce(missing_end, end_month) as missing_end,
		case when missing_start is null and missing_end is null then 0 else coalesce(missing_end, end_month) - coalesce(missing_start, start_month) end as total_missing_months
--		missing_start as missing_start,
--		missing_end as missing_end
	from missing_periods mp
)
--select * from fixed_missing_ends


select
	sr.subreddit, sr.created_utc, sr.max_months, mp.missing_start, mp.missing_end, total_missing_months, start_period_date, end_period_date,
	sr.created_utc + (interval '1 day' * ((sr.max_months-1) * 30)) as last_possible_day,
	first_missing_start, 
	case when first_missing_start is not null and first_missing_start < (sr.max_months-1) then 1 else 0 end as death

into s2_subreddit_deaths
from subreddit_ranges sr
	left join
	(
		select subreddit, min(missing_start) as first_missing_start
		from fixed_missing_ends
		where total_missing_months > 1
		group by subreddit
	) a on sr.subreddit = a.subreddit
left join missing_periods mp on mp.subreddit = sr.subreddit and a.first_missing_start = mp.missing_start;
	
--where total_missing_months > 1;


grant select on s2_subreddit_deaths to public;

create index on s2_subreddit_deaths(subreddit);
create index on s2_subreddit_deaths(missing_start);
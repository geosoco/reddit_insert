drop table if exists s2_m12_success_metrics;

with year_subs as (
	select 
		display_name, 
		created_utc,
		ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01'
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
monthly_joins as (
	select
		sm.subreddit,
		sm.creation_delta_months,
		count(*) as num_user_joins
	from sub_months sm
	left join s2_user_subreddit_joins usj on sm.subreddit = usj.subreddit and sm.creation_delta_months = usj.creation_delta_months
	group by sm.subreddit, sm.creation_delta_months
)
select
	sm.subreddit, 
	sm.creation_delta_months,
	s30.creation_delta_months as activity_month,
	sg30.creation_delta_months as gini_month,
	s30r.creation_delta_months as retention_month,
	mj.creation_delta_months as joins_month,

	coalesce(s30.total_activity, 0) as total_activity,
	coalesce(s30.total_submissions, 0) as total_submissions,
	coalesce(s30.total_comments, 0) as total_comments,
	coalesce(s30.unique_authors, 0) as unique_authors,
	
--	array_agg(unique_authors) over w1 as preceeding_authors,
	avg(coalesce(unique_authors,0)) over w1 as avg_authors,
	sum(unique_authors) over w1 as total_authors,

	count(s30.creation_delta_months) over w1 as num_12m_active_months,

	
--	array_agg(sg30.total_comments) over w1 as total_comments,
	avg(coalesce(sg30.total_comments,0)) over w1 as avg_comments,
	sum(sg30.total_comments) over w1 as cumsum_comments,

--	array_agg(sg30.total_submissions) over w1 as total_submissions,
	avg(coalesce(sg30.total_submissions,0)) over w1 as avg_submissions,
	sum(sg30.total_submissions) over w1 as cumsum_submissions,	

--	array_agg(round(coalesce(total_activity_gini,0), 2)) over w1 as arr_total_activity_gini,
	avg(coalesce(total_activity_gini,0)) over w1 as avg_activity_gini,
--	array_agg(round(coalesce(total_comments_gini,0), 2)) over w1 as arr_total_comments_gini,
	avg(coalesce(total_comments_gini,0)) over w1 as avg_comments_gini,
--	array_agg(round(coalesce(total_submissions_gini,0), 2)) over w1 as arr_total_submissions_gini,
	avg(coalesce(total_submissions_gini,0)) over w1 as avg_submissions_gini,

--	array_agg(coalesce(retention_rate,0)) over w1 as arr_retention_rate,
	avg(coalesce(retention_rate,0)) over w1 as avg_retention_rate,

	mj.num_user_joins as num_m12_user_joins

into s2_m12_success_metrics
from sub_months sm
left join s2_subreddit_30day_activity_summary s30 on s30.subreddit = sm.subreddit and s30.creation_delta_months = sm.creation_delta_months
left join s2_subreddit_gini_30days sg30 on sg30.subreddit = sm.subreddit and sg30.creation_delta_months = sm.creation_delta_months
left join subreddit_30day_retention_fixed s30r on sm.subreddit = s30r.subreddit and s30r.creation_delta_months = sm.creation_delta_months
left join monthly_joins mj on sm.subreddit = mj.subreddit and mj.creation_delta_months = sm.creation_delta_months

		window w as (partition by sm.subreddit order by sm.creation_delta_months asc),
		w1 as (partition by sm.subreddit order by sm.creation_delta_months range between 11 preceding and current row);


grant select on s2_m12_success_metrics to public;

create index on s2_m12_success_metrics(subreddit);
create index on s2_m12_success_metrics(subreddit, creation_delta_months);
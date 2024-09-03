with year_subs as (
	select display_name, created_utc
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
	from subreddit_summary ss
	inner join year_subs ys on ss.name  = ys.display_name
)
select *
from eligible_subs
where total_submissions >= 400




--select name, created_utc, total_activity, total_comments, total_submissions, submission_bin
--from eligible_subs
--where total_submissions >= 400
--order by submission_bin desc
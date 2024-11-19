--
--
-- s2_user_sub_retentive_activity_full
--
--


drop table if exists s2_user_sub_retentive_activity_full;


with test as (
select subreddit, author, min(creation_delta_months) as first_month, max(creation_delta_months) as last_month
from s2_sub_user_retention_intermediate
where author != '[deleted]'
group by subreddit, author
),
eligible_combos as (
select *
from test
where last_month-first_month > 2
),
periods as (
select
	subreddit, author, first_month, last_month, creation_delta_months
from eligible_combos
cross join lateral generate_series(first_month, last_month) m(creation_delta_months)
order by subreddit, author, creation_delta_months
),
total_data as (
select 
	p.*,
	coalesce(suri.total_activity, 0) as total_activity,
	coalesce(suri.total_submissions, 0) as total_submissions,
	coalesce(suri.total_comments, 0) as total_comments
from periods p
left join s2_sub_user_retention_intermediate suri on suri.subreddit = p.subreddit and suri.author = p.author and suri.creation_delta_months = p.creation_delta_months
)
select
*, 
lag(total_activity) over w  as prev_total_activity,
lead(total_activity) over w  as next_total_activity,

lag(total_submissions) over w  as prev_total_submissions,
lead(total_submissions) over w  as next_total_submissions,

lag(total_comments) over w  as prev_total_comments,
lead(total_comments) over w  as next_total_comments


into s2_user_sub_retentive_activity_full
from total_data
window w as (partition by subreddit, author order by creation_delta_months asc)
;


grant select on s2_user_sub_retentive_activity_full to public;



--select * from s2_sub_user_retention_intermediate limit 1000;


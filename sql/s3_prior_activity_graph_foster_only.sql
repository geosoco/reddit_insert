--
--
-- s3_prior_activity_graph_foster_only
--
--
--
-- Contains combinations for every subreddit of prior activity per FOSTERING author
--
-- So if an account participated in A, B, C in the 30 days prior to joining a political subreddit, this would have 3 rows
-- A->B
-- A->C
-- B->C
--
-- Because the cross join is done with a less-than, Cs would never be in sub1 and there would be no cyclical link
--








with 
fostering as (
	select
		subreddit, author
	from s3_user_joins_with_fostering_monthly
	where  first_activity_time <= (('2016-09-01')::date + interval '7' day) and first_fostering_month is not null

),
with_periods as (
	select 
		political_sub_first_content,
		s.author,
		join_date,
		case when join_date > '2016-07-29' then 3
		when join_date > '2016-02-01' then 2
		else 1 end as timeperiod,		
		s.subreddit
	from s3_activity_before_join_political_user_subreddit_summary s
	inner join fostering f on f.subreddit = s.political_sub_first_content and f.author = s.author
	where join_date < '2016-09-01'
),
combinations as (
select 
	s1.political_sub_first_content, s1.join_date, s1.timeperiod, s1.author, s1.subreddit as sub1, s2.subreddit as sub2
from with_periods s1
cross join with_periods s2 
where s1.political_sub_first_content = s2.political_sub_first_content and s1.author = s2.author
and s1.subreddit < s2.subreddit
)
select
political_sub_first_content, join_date, timeperiod, sub1, sub2, count(*) as weight
into s3_prior_activity_graph_foster_only
from combinations
group by political_sub_first_content, join_date, timeperiod, sub1, sub2;


grant select on s3_prior_activity_graph_foster_only to public;

create index on s3_prior_activity_graph_foster_only(political_sub_first_content);
--create index on s3_prior_activity_graph(sub1);
--create index on s3_prior_activity_graph(sub2);
create index on s3_prior_activity_graph_foster_only(weight);
create index on s3_prior_activity_graph_foster_only(political_sub_first_content, weight);


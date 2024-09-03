--
-- edges
--

select lower(b.source) as source, 
	lower(b.target) as target, 
	b.cnt as total_links
from 
(select lower(a.source) as source, lower(a.target) as target, count(*) as cnt from (
	with subs as (select lower(ss.name) as name from subreddit_summary ss
			  left join subreddits s on lower(ss.name) = lower(s.display_name)
	where ss.total_activity > 100 and ss.unique_authors > 10 and s.subreddit_type != 'user')

	select lower(subs.name) as source, lower((regexp_matches(s.data->>'description', '/?r/([a-z0-9_]+)', 'gi'))[1]) as target
	from subs
	left join subreddits s on lower(s.display_name) = subs.name) a
group by a.source, a.target) b



--
-- nodes 
--

select lower(a.name) as Id, s.display_name as Label, to_char(s.created_utc, 'YYYY-MM-DD"T"HH24:MI:SSOF') as Timeset, s.subscribers as subscribers, coalesce(ss.total_activity,0) as total_activity, coalesce(ss.unique_authors,0) as unique_authors from
(with subs as (select ss.name as name from subreddit_summary ss
			  left join subreddits s on lower(ss.name) = lower(s.display_name)
	where ss.total_activity > 100 and ss.unique_authors > 10 and s.subreddit_type != 'user')

	select lower(subs.name) as name from subs
	union 
	select lower((regexp_matches(s.data->>'description', '(?:/|[^\w])r/([a-z0-9_]+)', 'gi'))[1]) as name from subs
	left join subreddits s on lower(s.display_name) = lower(subs.name)
	) a

left join subreddits s on lower(s.display_name) = lower(a.name)
left join subreddit_summary ss on lower(ss.name) = lower(a.name)
where a.name is not null and s.display_name is not null and ss.name is not null;




--
-- combined
--
--
-- NOTE: this adds some subreddits to the list that don't meet thresholds because they are target subs
-- NOTE: these get filtered out later in the process. 

select lower(a.name) as Id, s.display_name as Label, to_char(s.created_utc, 'YYYY-MM-DD"T"HH24:MI:SSOF') as Timeset, s.subscribers as subscribers, coalesce(ss.total_activity,0) as total_activity, coalesce(ss.unique_authors,0) as unique_authors from
(with subs as (select ss.name as name from subreddit_summary ss
			  left join subreddits s on lower(ss.name) = lower(s.display_name)
	where ss.total_activity > 100 and ss.unique_authors > 10 and s.subreddit_type != 'user')

	select lower(subs.name) as name from subs
	union 
	select lower((regexp_matches(s.data->>'description', '(?:/|[^\w])r/([a-z0-9_]+)', 'gi'))[1]) as name from subs
	left join subreddits s on lower(s.display_name) = lower(subs.name)
	) a

left join subreddits s on lower(s.display_name) = lower(a.name)
left join subreddit_summary ss on lower(ss.name) = lower(a.name)
where a.name is not null and s.display_name is not null and ss.name is not null;
with subs as (select lower(ss.name) as name from subreddit_summary ss
			  left join subreddits s on lower(ss.name) = lower(s.display_name)
where ss.total_activity > 100 and ss.unique_authors > 1 and s.subreddit_type != 'user')

select subs.name as source, regexp_matches(s.data->>'description', '/?(r/[a-z0-9_]+)', 'gi') as target
from subs
left join subreddits s on lower(s.display_name) = subs.name;









select name, total_comments, total_submissions, unique_authors from subreddit_summary 
where total_activity > 100 and unique_authors > 1
limit 10;


select substr(display_name, 2) from subreddits limit 10;


select concat('r/', name) as name from subreddit_summary 
where total_activity > 100 and unique_authors > 1



select * from subreddits limit 1000;


select display_name, 
regexp_matches(data->>'description', '/?(r/[a-z0-9_]+)', 'gi')
from subreddits 
where subreddit_type != 'user';




with subs as (select display_name as name from subreddits)
select lower(substr(name,3)), count(*) as cnt, array_agg(name) as names from subs
group by lower(substr(name,3))
order by cnt desc;


select * from subreddits where lower(display_name) = 'seattle';



with subs as (select lower(display_name) as name from subreddits where subreddit_type != 'user'),
subs2 as (select lower(name) as name from subreddit_summary where total_activity > 100 and unique_authors > 1)
select subs.name
from subs
inner join subs2 on subs2.name = subs.name;

select subs.name
left join subreddit_summary ss on lower(ss.name) = subs.name
where 
ss.total_activity

select lower(ss.name) as name from subreddit_summary ss
left join subreddits s on lower(s.display_name) = lower(ss.name)
where ss.total_activity > 100 and ss.unique_authors > 1 and s.subreddit_type != 'user';





select id, regexp_matches(data->>'body', '(?:\W)(reddit\.com)'), data->>'body', created_utc from comments_y2012_m01
where data->>'body' ilike '%http://reddit.com%' and created_utc between '2012-01-01 00:00:00'::timestamp and '2012-01-02 00:00:00'::timestamp
limit 100;







with subs as (select lower(ss.name) as name from subreddit_summary ss
			  left join subreddits s on lower(ss.name) = lower(s.display_name)
where ss.total_activity > 100 and ss.unique_authors > 1 and s.subreddit_type != 'user')

select subs.name as source, (regexp_matches(s.data->>'description', '/?r/([a-z0-9_]+)', 'gi'))[1] as target
from subs
left join subreddits s on lower(s.display_name) = subs.name;

--
-- s2_cross_posts_30day_summary
--
--
--
--

drop table if exists s2_cross_posts_30day_summary;

with year_subs as (
	select display_name as subreddit, created_utc,
	ceil(extract(epoch from ('2016-12-02 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_months
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' 
),
sub_months as (
	select 
	ys.subreddit, 
	created_utc,
	max_months,
	month as creation_delta_months

	from year_subs ys
	cross join lateral generate_series(0, floor(ys.max_months)::int) m(month)
	order by ys.subreddit, month
),
summary_data as (
	select
		sm.subreddit as mentioned_sub_name,
		(extract(epoch from (cpi.created_utc - sm.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
		cpi.author,
		cpi.subreddit
	from sub_months sm
	left join cross_posts_intermediate cpi on lower(cpi.source_subreddit) = lower(sm.subreddit)
)
	select
		mentioned_sub_name,
		creation_delta_months,
		count(distinct author) as num_authors,
		count(distinct case when author != '[deleted]' then author else NULL end) as non_deleted_authors,
		count(distinct sd.subreddit) as num_subreddits,
		count(*) as total_cross_posts
	into s2_cross_posts_30day_summary
	from summary_data sd

	group by sd.mentioned_sub_name, creation_delta_months
	order by sd.mentioned_sub_name, creation_delta_months;



grant select on s2_cross_posts_30day_summary to public;

create index on s2_cross_posts_30day_summary(mentioned_sub_name);
create index on s2_cross_posts_30day_summary(mentioned_sub_name, creation_delta_months);
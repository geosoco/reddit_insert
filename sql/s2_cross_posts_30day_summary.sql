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
summary_data as (
	select
		ys.subreddit as mentioned_sub_name,
		(extract(epoch from (cpi.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
		cpi.author,
		cpi.subreddit,
		cpi.source_id
	from year_subs ys
	left join cross_posts_intermediate cpi on lower(cpi.source_subreddit) = lower(ys.subreddit)
)
	select
		ys.subreddit as mentioned_sub_name,
		sd.creation_delta_months,
		count(distinct author) as num_authors,
		count(distinct case when author != '[deleted]' then author else NULL end) as non_deleted_authors,
		count(distinct sd.subreddit) as num_subreddits,
		count(sd.source_id) as total_cross_posts
	into s2_cross_posts_30day_summary
	from year_subs ys
	left join summary_data sd on ys.subreddit = sd.mentioned_sub_name 
	where sd.creation_delta_months is not null

	group by ys.subreddit, sd.creation_delta_months
	order by ys.subreddit, sd.creation_delta_months;



grant select on s2_cross_posts_30day_summary to public;

create index on s2_cross_posts_30day_summary(mentioned_sub_name);
create index on s2_cross_posts_30day_summary(mentioned_sub_name, creation_delta_months);
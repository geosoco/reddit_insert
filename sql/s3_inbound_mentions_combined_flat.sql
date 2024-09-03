--
--
-- s3_inbound_mentions_combined_flat
--
--

drop table if exists s3_inbound_mentions_combined_flat;



with year_subs as (
	select 
		display_name,
		created_utc, 
		ceil(extract(epoch from ('2017-01-01 00:00:00'::timestamp - created_utc))/(24*60*60*30)) as max_days
	
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
),
sub_days as (
	select 
	ys.display_name as subreddit, 
	created_utc,
	max_days,
	day as creation_delta_days

	from year_subs ys
	cross join lateral generate_series(0, floor(ys.max_days)::int) m(day)
	order by ys.display_name, day
),
comment_ads as (
	select
		sld.id as comment_id,
		(NULL)::int as submission_id,
		ys.display_name as mentioned_sub_name,
		sld.subreddit,
		sld.created_utc,
		(extract(epoch from (sld.created_utc - ys.created_utc))::bigint) / (3600*24)::int as creation_delta_days,
		author,
		link_type,
		mentioned_sub_link,
		'c' as source,
		(NULL)::text as sub_source
	
	from year_subs ys
	left join s2_comment_sub_link_details sld on sld.mentioned_sub_name = lower(ys.display_name)
	where self_reference != TRUE 
),
submission_links as (
select
	(NULL)::int as comment_id,
	a.id as submission_id,
	a.mentioned_sub_name,
	a.subreddit, 
	a.created_utc,
	(extract(epoch from (a.created_utc - ys.created_utc))::bigint) / (3600*24)::int as creation_delta_days,
	a.author,
	a.link_type,
	a.mentioned_sub_link,
	a.source,
	a.sub_source
	
	from year_subs ys
	left join ( 
		select
			stsld.id, ys.display_name as mentioned_sub_name, stsld.subreddit, stsld.created_utc, 
			stsld.author, stsld.link_type, stsld.mentioned_sub_link, 
			's' as source, 'title' as sub_source
			
		from year_subs ys 
		left join s2_submission_title_sub_link_details stsld on lower(ys.display_name) = stsld.mentioned_sub_name
		where stsld.self_reference != TRUE 
		union all
		select
			distinct sssld.id, ys.display_name as mentioned_sub_name, sssld.subreddit, sssld.created_utc,
			sssld.author, sssld.link_type, sssld.mentioned_sub_link, 's' as source, 'selftext' as sub_source
		from year_subs ys 
		left join s2_submission_selftext_sub_link_details sssld on lower(ys.display_name) = sssld.mentioned_sub_name
		where sssld.self_reference != TRUE 
	) a	on a.mentioned_sub_name = ys.display_name
	--left join s2_submission_title_sub_link_details stsld on stsld.id = a.id
	--left join s2_submission_selftext_sub_link_details sssld on sssld.id = a.id
)
select *
into s3_inbound_mentions_combined_flat
from (
	select * from comment_ads 
	union all 
	select * from submission_links) a;


grant select on s3_inbound_mentions_combined_flat to public;

create index on s3_inbound_mentions_combined_flat(mentioned_sub_name);
create index on s3_inbound_mentions_combined_flat(subreddit);
create index on s3_inbound_mentions_combined_flat(comment_id);
create index on s3_inbound_mentions_combined_flat(submission_id);




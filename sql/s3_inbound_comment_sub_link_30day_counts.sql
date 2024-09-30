--
--
-- s3_inbound_comment_sub_link_30day_counts
--
--
--
--
--


drop table if exists s3_inbound_comment_sub_link_30day_counts;

with year_subs as (
	select display_name, created_utc
	from subreddits
	where display_name in ('The_Donald', 'hillaryclinton', 'SandersForPresident')
)
select
	subreddit,
	author,
	ys.display_name as mentioned_sub_name,
	(extract(epoch from (sld.created_utc - ys.created_utc))::bigint) / (3600*24*30)::int as creation_delta_months,
	count(*) as total_links
	
into s3_inbound_comment_sub_link_30day_counts
from year_subs ys 
left join s2_comment_sub_link_details sld on sld.mentioned_sub_name = lower(ys.display_name)
where self_reference != TRUE and mentioned_sub_name is not null
group by subreddit, author, ys.display_name, creation_delta_months;

create index on s3_inbound_comment_sub_link_30day_counts(subreddit);
create index on s3_inbound_comment_sub_link_30day_counts(mentioned_sub_name);
create index on s3_inbound_comment_sub_link_30day_counts(author);



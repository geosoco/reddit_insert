--
-- default_sub_submissions_meta
--
--
-- WARNING - In the reddit data, it looks like reddit is changing any removed links to be a self_post, which may
-- artificially inflate some analyses which use it
-- deleted links should still have the url and be is_self = true


drop table if exists default_sub_submissions_details;




select
	dsm.subreddit,
	created_utc,
	author,
	s.id, 
	(data->>'is_self')::bool as is_text_post,
	(data->>'selftext' is not null AND 
	 data->>'selftext' != '[deleted]' AND
	 data->>'selftext' != '[removed]' AND
	 length(data->>'selftext') > 0
	) as has_body_text,
	data->>'selftext' = '[deleted]' as has_deleted_text,
	data->>'selftext' = '[removed]' as has_removed_text,
 
	(data->>'score')::integer as score, 
	(data->>'num_comments')::integer as num_comments_from_data
	into default_sub_submissions_details
	from default_subreddit_meta dsm
	left join submissions s on dsm.subreddit = s.subreddit
	where
	
	dsm.included = 'yes'

	and age(date_trunc('month', created_utc)::timestamp without time zone, date_trunc('month', dsm.added)) >= interval '-24 month'
	and age(date_trunc('month', created_utc)::timestamp without time zone, date_trunc('month', dsm.added)) < interval '25 month';



grant select on default_sub_submissions_details to public;

create index on default_sub_submissions_details(subreddit);
create index on default_sub_submissions_details(author);
create index on default_sub_submissions_details(id);
create index on default_sub_submissions_details(created_utc);
create index on default_sub_submissions_details(subreddit, author);
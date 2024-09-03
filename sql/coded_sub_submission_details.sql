
drop table if exists coded_sub_submissions_details;




select
	s.subreddit,
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
	(data->>'num_comments')::integer as num_comments_from_data,
	data->>'title' as title,
	data->>'url' as url,
	data->>'domain' as domain,
	data->>'selftext' as selftext,
	data
	into coded_sub_submissions_details
	from submissions s
	where s.subreddit in ('AnimalsFailing', 'EarthScience', 'Eskrima', 'LinusTechTips', 'WomensSoccer', 'AskWomenOver30', 'UpliftingNews', 'Fzero');



grant select on coded_sub_submissions_details to public;

create index on coded_sub_submissions_details(subreddit);
create index on coded_sub_submissions_details(author);
create index on coded_sub_submissions_details(id);
create index on coded_sub_submissions_details(created_utc);
create index on coded_sub_submissions_details(subreddit, author);
create index on coded_sub_submissions_details(title);


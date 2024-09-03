with datetimes as (
	select
		created_utc, 
		subreddit, 
		data->>'score' as score,
		to_timestamp( (data->>'retrieved_on')::int ) as retrieved_on, 
		to_timestamp( (data->>'retrieved_on')::int ) - created_utc as retrieval_time
	from coded_sub_submissions_details
	where data->>'retrieved_on' is not null
)
select min(created_utc) from datetimes
where retrieval_time < interval '60 day'

-- earliest submission with a retrieval time less than 90 days is 2015-05-29 20:03:13
-- for 60 days its 2015-06-28 03:27:23



with dates as (
select 
	created_utc, data->>'retrieved_on'
from coded_sub_submissions_details
where 
	data->>'retrieved_on' is  null
)
select min(created_utc), max(created_utc) from dates

-- this returns min = 2011-03-08-02:31:17 and max = 2012-08-31 20:18:34
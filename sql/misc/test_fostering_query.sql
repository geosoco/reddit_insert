--
-- test_fostering_query
--
-- A simple test to make sure the fostering logic and identification
--
--
--


drop table if exists test_fostering_query;

create temporary table if not exists  test_fostering_query (
	subreddit 				char(32),
	author 					char(32),
	creation_delta_months 	int,
	total_activity			int
);


insert into test_fostering_query 
values

	('testsub', 'sue', 0, 1),

	('testsub', 'ted', 0, 10),

	('testsub', 'kim', 2, 10),

	('testsub', 'ben', 2, 1),

	('testsub', 'nic', 0, 10),
	('testsub', 'nic', 1, 11),
	('testsub', 'nic', 2, 12),

	('testsub', 'may', 4, 10),
	('testsub', 'may', 5, 11),
	('testsub', 'may', 6, 12),

	('testsub', 'liz', 0, 1),
	('testsub', 'liz', 1, 10),
	('testsub', 'liz', 2, 11),
	('testsub', 'liz', 3, 2),
	('testsub', 'liz', 4, 5),
	('testsub', 'liz', 6, 10),

	('testsub', 'zeb', 2, 10),
	('testsub', 'zeb', 4, 11),
	('testsub', 'zeb', 5, 12),

	('testsub', 'kat', 6, 11),
	('testsub', 'kat', 7, 10),
	('testsub', 'kat', 8, 1),

	('testsub', 'jim', 0, 10),
	('testsub', 'jim', 1, 11),
	('testsub', 'jim', 3, 12),

	('testsub', 'mar', 1, 10),
	('testsub', 'mar', 2, 2), 
	('testsub', 'mar', 3, 11)
;



with sub_user_monthly_retention_intermediate as (
	select
		author,
		subreddit, 
		creation_delta_months,
		lead(creation_delta_months) over w as next_active_month,
		case when lead(creation_delta_months) over w = creation_delta_months +  1 then lead(total_activity) over w else 0 end as next_month_activity,
		case when lag(creation_delta_months) over w = creation_delta_months - 1 then lag(total_activity) over w else 0 end as prev_month_activity,
		total_activity
	from test_fostering_query
	-- where author != '[deleted]'	 
	window w as (partition by subreddit, author order by creation_delta_months asc)
),
seq_data as (
	-- The numbers below can be one so long as the data above filters to the proper threshold
	-- meaning that this will already only consider rows that are 

	select
		*, 
		case when prev_month_activity < 5 and total_activity >= 5 then 1 else NULL end as seq_start,
		case when total_activity >= 5 and next_month_activity < 5 then 1 else NULL end as seq_end
	
		from sub_user_monthly_retention_intermediate
		where author != '[deleted]' 
),
boundaries_table as (
	select 
		*,

		case when total_activity < 5 then null else
		0 + sum(seq_start) over (partition by subreddit, author order by creation_delta_months) 
		end	as seq_id
	
	from seq_data
)
-- select * from boundaries_table
select
	subreddit, author, seq_id, 
	min(case when seq_start = 1 then creation_delta_months else NULL end) as first_delta_month,
	max(case when seq_end = 1 then creation_delta_months else NULL end) as last_delta_month,
	count(*) as total_months,
	sum(total_activity) as total_activity
from boundaries_table

where seq_id is not null
group by subreddit, author, seq_id
order by subreddit, author, seq_id;


-- drop the table
drop table if exists test_fostering_query;
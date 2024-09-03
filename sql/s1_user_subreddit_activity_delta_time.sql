drop table if exists s1_user_subreddit_activity_delta_time;

with 
active_users as (
	select subreddit, author, total_activity
	from user_subreddit_activity usa
	where subreddit in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
	and author != '[deleted]'
	and total_activity > 1

),
user_data as (
	select uca.*
	from active_users au
	left join user_combined_activity uca on uca.subreddit = au.subreddit and uca.author = au.author
),
between_time as (
	select
		*,
		row_number() over (partition by subreddit, author order by created_utc asc) as number,
		created_utc - lag(created_utc) over (partition by subreddit, author order by created_utc asc) as delta_time
	from user_data
)
select *
into s1_user_subreddit_activity_delta_time
from between_time
;


grant select on s1_user_subreddit_activity_delta_time to public;
with sub_data as (
	select
		s.id,
		s.subreddit,
		s.created_utc,
		sr.created_utc as sub_creation_date,
		extract(epoch from s.created_utc - sr.created_utc) as seconds_since_creation,
		(extract(epoch from s.created_utc - sr.created_utc)/(24.0*3600.0)) as days_since_creation,
		s.author,
		s.data->>'title' as title,
		s.data->>'num_comments' as num_comments,
		s.data->>'score' as score
	from submissions s
	left join subreddits sr on s.subreddit = sr.display_name
	where s.subreddit in ('AskWomenOver30','WomensSoccer','UpliftingNews','EarthScience','MonkeyIsland','LinusTechTips','AnimalsFailing','Eskrima')
)
select 
	row_number() over(partition by subreddit order by sd.created_utc asc) as submission_number,
	sd.*
into submissions_coded
from sub_data sd

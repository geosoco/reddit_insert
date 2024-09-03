drop table if exists coded_subs_unique_authors_30days;

with sub_data as (
	select display_name, created_utc
	from subreddits
	where display_name in ('AnimalsFailing', 'Fzero', 'EarthScience', 'Eskrima', 'LinusTechTips', 'WomensSoccer', 'AskWomenOver30', 'UpliftingNews')

), 
activities as (
	select subreddit, author, created_utc
	from user_combined_activity ucd
	where subreddit in ('AnimalsFailing', 'Fzero', 'EarthScience', 'Eskrima', 'LinusTechTips', 'WomensSoccer', 'AskWomenOver30', 'UpliftingNews')
)

select 
	subreddit,
	(floor( (a.created_utc::date - sd.created_utc::date)/30 ))::int as rel_month,
	count(distinct author) as unique_authors
into coded_subs_unique_authors_30days
from activities a
inner join sub_data sd on sd.display_name = a.subreddit
group by subreddit, rel_month;



--vacuum freeze analyze;

grant select on coded_subs_unique_authors_30days to public;

create index on coded_subs_unique_authors_30days(subreddit);
create index on coded_subs_unique_authors_30days(author);

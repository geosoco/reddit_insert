--
-- s1 retention calculations
--

with subreddit_creation as (
	select display_name, created_utc from
	subreddits
	where display_name in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
),	
lead_data as (
	select
		author,
		subreddit, 
		creation_delta_months,
		lead(creation_delta_months) over (partition by subreddit, author order by creation_delta_months asc) as next_active_month,
		case when lead(creation_delta_months) over (partition by subreddit, author order by creation_delta_months asc) = creation_delta_months+1 then 1 else 0 end as active_next_month,
		total_activity,
		total_submissions,
		total_comments
	from subreddit_creation sc
		left join user_sub_activity_30day_activity usa on usa.subreddit = sc.display_name
	where total_submissions > 0
),
submission_data as (
	select floor(extract(day from cssd.created_utc - sc.created_utc)/30) as month,
	cssd.id, cssd.subreddit, cssd.author, cssd.created_utc, cssd.num_comments_from_data, cssd.score
	from subreddit_creation sc
	left join coded_sub_submissions_details cssd on cssd.subreddit = sc.display_name
), 
combined_data as (
	select ld.author, ld.subreddit, ld.creation_delta_months, ld.active_next_month, sd.month, sd.id, sd.author, sd.num_comments_from_data, sd.score
	from lead_data ld
	left join submission_data sd on sd.subreddit = ld.subreddit and sd.author = ld.author and sd.month = ld.creation_delta_months
)
select
	subreddit, creation_delta_months,  
	sum(num_comments_from_data) as total_comments, 
	sum(score) as total_score, 
	coalesce(sum(case when active_next_month = 1 then score else 0 end),0) as total_retentive_score,
	coalesce(sum(case when active_next_month = 0 then score else 0 end),0) as total_nonretentive_score,
	coalesce(sum(case when active_next_month = 1 then num_comments_from_data else 0 end),0) as retentive_comments,
	coalesce(sum(case when active_next_month = 0 then num_comments_from_data else 0 end),0) as nonretentive_comments,

	coalesce(avg(case when active_next_month = 1 then score else NULL end),0) as avg_total_retentive_score,
	coalesce(avg(case when active_next_month = 0 then score else NULL end),0) as avg_total_nonretentive_score,
	coalesce(avg(case when active_next_month = 1 then num_comments_from_data else NULL end),0) as avg_retentive_comments,
	coalesce(avg(case when active_next_month = 0 then num_comments_from_data else NULL end),0) as avg_nonretentive_comments,
	
	avg(num_comments_from_data), avg(score)
from combined_data
group by subreddit, creation_delta_months 
order by subreddit, creation_delta_months
		
	



select 
	t1.subreddit, t1.creation_delta_months,
	t1.total_active_authors, t2.total_active, sdr.total_active,
	t1.retention_rate, t2.retention_rate, sdr.retention_rate
from s1_subreddit_30day_retention_fixed2 t1
full outer join s1_subreddit_30day_retention_fixed t2 on t1.subreddit = t2.subreddit and t1.creation_delta_months = t2.creation_delta_months
full outer join subreddit_30day_retention sdr on sdr.subreddit = t1.subreddit and t1.creation_delta_months = sdr.creation_delta_months
	where sdr.subreddit in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima') 
order by t1.subreddit, t1.creation_delta_months



--create index on user_sub_activity_30day_activity(subreddit);




select
	t1.subreddit, t1.author, t1.creation_delta_months t1_delta_month, t2.creation_delta_months as t2_delta_month,
	t1.*,
	t2.*
from user_sub_activity_30day_activity t1
full outer join user_sub_activity_30day_activity t2 on t2.subreddit = t1.subreddit and t2.author = t1.author and t2.creation_delta_months-1 = t1.creation_delta_months
where 
	t1.subreddit in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima') 
	and t2.subreddit in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
	and t1.author != '[deleted]' and t2.author != '[deleted]'
	and t1.creation_delta_months is null
limit 10000;






with lead_data as (
select
	author,
	subreddit, 
	creation_delta_months,
	lead(creation_delta_months) over (partition by subreddit, author order by creation_delta_months asc) as next_active_month,
	case when lead(creation_delta_months) over (partition by subreddit, author order by creation_delta_months asc) = creation_delta_months+1 then 1 else 0 end as active_next_month,
	total_activity,
	total_submissions,
	total_comments
from user_sub_activity_30day_activity
where subreddit in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
), sub_counts as (
select subreddit, creation_delta_months, 
	count(distinct author) as unique_authors,
	sum(total_activity) as total_activity,
	sum(total_comments) as total_comments,
	sum(total_submissions) as total_submissions,

	sum(case when total_comments > 0 and total_submissions > 0 then 1 else 0 end) as sub_and_comment_authors,
	sum(case when total_activity = total_comments then 1 else 0 end) as comment_only_authors,
	sum(case when total_activity = total_submissions then 1 else 0 end) as submission_only_authors,


	sum(active_next_month) as retentive_authors,
	sum(case when active_next_month = 1 then total_activity else 0 end) as retentive_activity,
	sum(case when active_next_month = 1 then 0 else total_activity end) as non_retentive_activity,
	sum(case when active_next_month = 1 then total_comments else 0 end) as retentive_comments,
	sum(case when active_next_month = 1 then total_submissions else 0 end) as retentive_submissions,


	
	sum(case when active_next_month = 1 and total_comments = total_activity then total_activity else 0 end) as retentive_commenter_only_activity,
	sum(case when active_next_month = 1 and total_submissions = total_activity then total_activity else 0 end) as retentive_submitter_only_activity,
	sum(case when active_next_month = 1 and total_comments > 0 and total_submissions > 0 then 1 else 0 end) as ret_sub_and_comment_authors,
	sum(case when active_next_month = 1 and total_comments > 0 and total_submissions > 0 then total_activity else 0 end) as ret_sub_and_comment_activity,
	sum(case when active_next_month = 1 and total_activity = total_comments then 1 else 0 end) as ret_comment_only_authors,
	sum(case when active_next_month = 1 and total_activity = total_comments then total_comments else 0 end) as ret_comment_only_activity,
	sum(case when active_next_month = 1 and total_activity = total_submissions then 1 else 0 end) as ret_submission_only_authors,
	sum(case when active_next_month = 1 and total_activity = total_submissions then total_submissions else 0 end) as ret_submission_only_activity,
	
	
	sum(case when author = '[deleted]' then total_activity else NULL end) as deleted_activity,
	sum(case when author = '[deleted]' then total_submissions else NULL end) as deleted_submissions,
	sum(case when author = '[deleted]' then total_comments else NULL end) as deleted_comments
	
from lead_data ld 
group by subreddit, creation_delta_months
)
select * from sub_counts


	
select subreddit,
	sum(total_activity) as total_activity,
	sum(total_comments) as total_comments,
	sum(total_submissions) as total_submissions,

	sum(retentive_activity) as retentive_activity,
	sum(retentive_comments) as retentive_comments,
	sum(retentive_submissions) as retenttive_submissions,

	sum(retentive_activity) *100.0 / sum(total_activity) as pct_retentive_activity,
	sum(retentive_comments) *100.0 / sum(total_comments) as pct_retentive_comments,
	sum(retentive_submissions) *100.0 / sum(total_submissions) as pct_retenttive_submissions
from sub_counts
group by subreddit



-- select
-- 	subreddit, creation_delta_months, unique_authors, retentive_authors, 
-- 	total_activity, retentive_activity, 
-- 	total_comments, retentive_comments, 
-- 	total_submissions, retentive_submissions,
	
-- 	retentive_activity*100.0/total_activity as pct_retentive_activity,
-- 	retentive_activity * 100.0 / (total_activity+coalesce(deleted_activity,0)) as pct_retentive_activity_with_deleted,

-- 	retentive_comments*100.0/total_activity as pct_comments_activity,
-- 	retentive_comments * 100.0 / (total_activity+coalesce(deleted_activity,0)) as pct_retentive_comments_with_deleted,
-- 	retentive_submissions*100.0/total_activity as pct_submissions_activity,
-- 	retentive_submissions * 100.0 / (total_activity+coalesce(deleted_activity,0)) as pct_retentive_submissions_with_deleted,


	
-- 	retentive_commenter_only_activity *100.0 / total_activity,
-- 	retentive_submitter_only_activity *100.0 / total_activity
	
	
-- from sub_counts
	


	
-- select 
-- 	subreddit, 
-- 		sum(unique_authors),
-- 		sum(retentive_authors),
-- 		sum(ret_sub_and_comment_authors),
-- 		sum(ret_comment_only_authors),
-- 		sum(ret_submission_only_authors)
	
-- from sub_counts sc
-- group by subreddit


--	from lead_data ld
--	left join sub_counts sc on sc.subreddit = lc



--select * from user_sub_activity_30day_activity where author = '[deleted]' limit 10;




with subreddit_creation as (
	select display_name, created_utc from
	subreddits
	where display_name in ('AskWomenOver30', 'WomensSoccer', 'UpliftingNews', 'EarthScience', 'Fzero', 'LinusTechTips', 'AnimalsFailing', 'Eskrima')
),	
lead_data as (
	select
		author,
		subreddit, 
		creation_delta_months,
		lead(creation_delta_months) over (partition by subreddit, author order by creation_delta_months asc) as next_active_month,
		case when lead(creation_delta_months) over (partition by subreddit, author order by creation_delta_months asc) = creation_delta_months+1 then 1 else 0 end as active_next_month,
		total_activity,
		total_submissions,
		total_comments
	from subreddit_creation sc
		left join user_sub_activity_30day_activity usa on usa.subreddit = sc.display_name
	where total_submissions > 0
),
submission_data as (
	select floor(extract(day from cssd.created_utc - sc.created_utc)/30) as month,
	cssd.id, cssd.subreddit, cssd.author, cssd.created_utc, cssd.num_comments_from_data, cssd.score
	from subreddit_creation sc
	left join coded_sub_submissions_details cssd on cssd.subreddit = sc.display_name
), 
combined_data as (
	select ld.author, ld.subreddit, ld.creation_delta_months, ld.active_next_month, sd.month, sd.id, sd.author, sd.num_comments_from_data, sd.score
	from lead_data ld
	left join submission_data sd on sd.subreddit = ld.subreddit and sd.author = ld.author and sd.month = ld.creation_delta_months
)
select
	subreddit, creation_delta_months,  
	sum(num_comments_from_data) as total_comments, 
	sum(score) as total_score, 
	coalesce(sum(case when active_next_month = 1 then score else 0 end),0) as total_retentive_score,
	coalesce(sum(case when active_next_month = 0 then score else 0 end),0) as total_nonretentive_score,
	coalesce(sum(case when active_next_month = 1 then num_comments_from_data else 0 end),0) as retentive_comments,
	coalesce(sum(case when active_next_month = 0 then num_comments_from_data else 0 end),0) as nonretentive_comments,

	coalesce(avg(case when active_next_month = 1 then score else NULL end),0) as avg_total_retentive_score,
	coalesce(avg(case when active_next_month = 0 then score else NULL end),0) as avg_total_nonretentive_score,
	coalesce(avg(case when active_next_month = 1 then num_comments_from_data else NULL end),0) as avg_retentive_comments,
	coalesce(avg(case when active_next_month = 0 then num_comments_from_data else NULL end),0) as avg_nonretentive_comments,
	
	avg(num_comments_from_data), avg(score)
from combined_data
group by subreddit, creation_delta_months 
order by subreddit, creation_delta_months
		
	







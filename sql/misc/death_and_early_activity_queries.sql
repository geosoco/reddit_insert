--
--
-- misc data about subreddit deaths and early activity, probably used as
-- a prefix to exclude possible subreddits depeneing on early activity or to understand
-- the relationship between perceived death states and overall activity within the subreddit
-- relative to early activity.
--
-- 


with sub_deaths as (
	select * from s2_subreddit_deaths where death = 0 OR missing_start > 5
),
early_activity as (
	select
		sd.subreddit as subreddit2, 
			avg(unique_authors) as avg_monthly_authors,
			avg(total_comments) as avg_total_comments,
			avg(total_submissions) as avg_total_submissions,
			avg(retentive_authors) as avg_retentive_authors,
			coalesce(avg(first_month_total_activity),0) as first_month_total_activity,
			coalesce(avg(first_month_unique_authors),0) as first_month_unique_authors,
			coalesce(avg(first_month_total_comments),0) as first_month_total_comments,
			sum(case when total_fostering_activity > 0 then 1 else 0 end) as total_fostered_months,
			avg(num_fostering_authors) as avg_fostering_authors,
			sum(num_fostering_authors) as total_fostering_authors,
			avg(total_fostering_activity) as avg_fostering_activity,
			sum(total_fostering_submissions) as total_fostering_submissions,
			sum(total_fostering_activity) as total_fostering_activity,
			sum(num_seed_authors) as total_seed_authors,
			sum(num_seeded_content) as total_seeded_content,
			sum(num_comment_adv_subreddits) as num_comment_adv_subreddits,
			sum(num_submission_title_adv_subreddits) as num_submission_title_adv_subreddits,
			sum(num_submission_selftext_adv_subreddits) as num_submission_selftext_adv_subreddits,
			bool_or(case when creator_seeding > 0 then TRUE else FALSE end) as creator_seeding,
			bool_or(case when num_seeding_mods > 0 then TRUE else FALSE end) as moderator_seeding
	
	from sub_deaths sd
	left join s2_subreddit_monthly_data_combined smdc on sd.subreddit = smdc.subreddit
	where smdc.creation_delta_months < 6
	group by sd.subreddit
),
activity as (
	select
		sd.subreddit as subreddit2, 
			avg(unique_authors) as avg_monthly_authors,
			avg(total_comments) as avg_total_comments,
			avg(total_submissions) as avg_total_submissions,
			avg(retentive_authors) as avg_retentive_authors,
			coalesce(avg(first_month_total_activity),0) as first_month_total_activity,
			coalesce(avg(first_month_unique_authors),0) as first_month_unique_authors,
			coalesce(avg(first_month_total_comments),0) as first_month_total_comments,
			sum(case when total_fostering_activity > 0 then 1 else 0 end) as total_fostered_months,
			avg(num_fostering_authors) as avg_fostering_authors,
			sum(num_fostering_authors) as total_fostering_authors,
			avg(total_fostering_activity) as avg_fostering_activity,
			sum(total_fostering_submissions) as total_fostering_submissions,
			sum(total_fostering_activity) as total_fostering_activity,
			sum(num_seed_authors) as total_seed_authors,
			sum(num_seeded_content) as total_seeded_content,
			sum(num_comment_adv_subreddits) as num_comment_adv_subreddits,
			sum(num_submission_title_adv_subreddits) as num_submission_title_adv_subreddits,
			sum(num_submission_selftext_adv_subreddits) as num_submission_selftext_adv_subreddits,
			bool_or(case when creator_seeding > 0 then TRUE else FALSE end) as creator_seeding,
			bool_or(case when num_seeding_mods > 0 then TRUE else FALSE end) as moderator_seeding
	
	from sub_deaths sd
	left join s2_subreddit_monthly_data_combined smdc on sd.subreddit = smdc.subreddit
	where (death = 1 AND smdc.creation_delta_months < sd.missing_start) OR (death =0)
	group by sd.subreddit
)

select 
	sd.*, 
	ea.*
from sub_deaths sd
left join early_activity ea on ea.subreddit2 = sd.subreddit
--left join activity a on a.subreddit2 = sd.subreddit

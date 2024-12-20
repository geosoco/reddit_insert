---------------------------------------------------------------------------
--
-- s2_subreddit_monthly_data_combined
--
---------------------------------------------------------------------------


drop table if exists s2_subreddit_monthly_data_combined;


with year_subs as (
	select display_name as subreddit, created_utc, subscribers
	from subreddits
	where created_utc >= '2012-01-01' and created_utc < '2013-01-01' and display_name is not null
),
creators as (
	select distinct subreddit, creator
	from subreddit_creator_updates
	where creator != '[deleted]'
),
mods as (
	select distinct subreddit, moderator
	from subreddit_moderator_updates
	where moderator != '[deleted]'
),
creator_and_mod_subs as (
	select
		subreddit, sum(num_creators) as num_creators, sum(num_moderators) as num_moderators
	from (
	
		select c.subreddit as subreddit, count(creator) as num_creators, NULL as num_moderators
			from creators c
			group by subreddit
		
		union
		
		select m.subreddit as subreddit, 0 as num_creators, count(moderator) as num_moderators
			from mods m
			group by subreddit
		
	) k
		
	group by subreddit
),
eligible_subs as (
	select ss.name, ys.created_utc, total_activity, total_comments, total_submissions, unique_authors, 
		case when total_submissions < 1000 then 0
		when total_submissions < 2627 then 1
		when total_submissions < 6443 then 2
		else 3
		end as submission_bin
	from year_subs ys 
	inner join subreddit_summary ss on ss.name  = ys.subreddit 
	--where total_submissions >= 400
),
first_6mo_sums as (
	select ys.subreddit,
		sum(total_activity) as total_activity,
		sum(total_comments) as total_comments,
		sum(total_submissions) as total_submissions,
		count(distinct author) as unique_authors
	from year_subs ys
	left join user_sub_activity_30day_activity usa on usa.subreddit = ys.subreddit
	where usa.creation_delta_months < 6
	group by ys.subreddit
),
seeding_content as (
	select s2.subreddit, 
	count(distinct s2.author) as num_seed_authors, 
	sum(s2.total_submissions) num_seeded_content, 
	sum(s2.is_mod) num_seeding_mods, 
	sum(case when cr.creator is not null then 1 else 0 end) as creator_seeding
	from year_subs ys
	left join s2_sub_user_retention_intermediate s2 on ys.subreddit = s2.subreddit
	left join creators cr on cr.subreddit = s2.subreddit and cr.creator = s2.author
	where creation_delta_months = 0 and s2.total_submissions >= 5 and s2.author != '[deleted]'
	group by s2.subreddit
	order by s2.subreddit asc	
),
fostering_data as (
	select
		s.subreddit, 
		s.creation_delta_months,
	
		count(distinct susd.author) as num_fostering_authors,
		sum(s.total_activity) as total_fostering_activity,
		sum(s.total_submissions) as total_fostering_submissions,
		sum(s.total_comments) as total_fostering_comments,
		sum(case when m.moderator is not null then 1 else 0 end) as num_fostering_mods,
		sum(case when m.moderator is not null then s.total_activity else 0 end) as total_moderator_fostering_activity,
		sum(case when m.moderator is not null then s.total_submissions else 0 end) as total_moderator_fostering_submissions,
		sum(case when m.moderator is not null then s.total_comments else 0 end) as total_moderator_fostering_comments,
		sum(case when c.creator is not null then 1 else 0 end) as num_fostering_creators,
		sum(case when c.creator is not null then s.total_activity else 0 end) as total_creator_fostering_activity,
		sum(case when c.creator is not null then s.total_submissions else 0 end) as total_creator_fostering_submissions,
		sum(case when c.creator is not null then s.total_comments else 0 end) as total_creator_fostering_comments
		
	from s2_sub_user_retention_intermediate s
	left join s2_sub_user_sequence_data susd on s.subreddit = susd.subreddit and s.author = susd.author
	left join creators c on c.subreddit = s.subreddit and c.creator = susd.author
	left join mods m on m.subreddit = s.subreddit and m.moderator = susd.author
	where susd.total_months >= 3 and susd.total_activity > (total_months * 10) 
	and susd.first_delta_month <= s.creation_delta_months and susd.last_delta_month >= s.creation_delta_months
	
	group by s.subreddit, s.creation_delta_months
),
cumulative_data as (
	select
		s2.subreddit,
		s2.creation_delta_months,

		s2.total_activity,
		s2.total_comments,
		s2.unique_authors,

		sum(s2.total_activity) over w as s2_cum_activity,
		sum(s2.total_comments) over w as s2_cum_comments,
		sum(s2.total_submissions) over w as s2_cum_submissions,
		sum(s2.unique_authors) over w as s2_cum_unique_authors,

		avg(s2.total_activity) over w as s2_avg_activity,
		avg(s2.total_comments) over w as s2_avg_comments,
		avg(s2.total_submissions) over w as s2_avg_submissions,
		avg(s2.unique_authors) over w as s2_avg_authors,

		avg(s2.total_activity) over w1 as s2_avg_activity2,
		avg(s2.total_comments) over w1 as s2_avg_comments2,
		avg(s2.unique_authors) over w1 as s2_avg_authors2

		from eligible_subs es
	    left join s2_subreddit_30day_activity_summary s2 on s2.subreddit = es.name
		window w as (partition by s2.subreddit order by s2.creation_delta_months asc),
		w1 as (partition by s2.subreddit order by s2.creation_delta_months range unbounded preceding)

),
first_month as (
	select 
		s0.subreddit,
		s0.total_activity as first_month_total_activity,
		coalesce(s0.total_submissions, 0) as first_month_total_submissions,
		coalesce(s0.total_comments, 0) as first_month_total_comments,
		coalesce(s0.unique_authors, 0) as first_month_unique_authors,
		coalesce(sg30.total_activity_gini, 0) as first_month_activity_gini,
		coalesce(sg30.total_comments_gini, 0) as first_month_comments_gini,
		coalesce(sg30.total_submissions_gini, 0) as first_month_submissions_gini

	from eligible_subs es 
	left join s2_subreddit_30day_activity_summary s0 on s0.subreddit = es.name
	left join s2_subreddit_gini_30days sg30 on sg30.subreddit = es.name and s0.creation_delta_months = sg30.creation_delta_months
	where s0.creation_delta_months = 0 and s0.total_activity > 0
)
-- advertising_comments as (
-- 	-- we're looking for inbound links here so subreddit is the mentioned_sub
-- 	select
-- 		mentioned_sub_name as subreddit,
-- 		creation_delta_months,
-- 		count(distinct author) as num_comment_adv_authors,
-- 		count(distinct subreddit) as num_comment_adv_subreddits,
-- 		sum(total_links) as total_comment_adv_links
	
-- 	from s2_comment_sub_link_30day_counts
-- 	where mentioned_sub_name is not null
-- 	group by mentioned_sub_name, creation_delta_months
-- ),
-- advertising_submission as (
-- select
-- 	mentioned_sub_name,
-- 	creation_delta_months,
	
-- 	num_comment_adv_authors,
-- 	num_comment_adv_subreddits,
-- 	total_comment_advs_total_links,
-- 	total_comment_adv_comments,

-- 	num_submission_title_adv_authors,
-- 	num_submission_title_adv_subreddits,
-- 	total_submission_title_advs_total_links,
-- 	total_title_adv_submissions,

-- 	num_submission_selftext_adv_authors,
-- 	num_submission_selftext_adv_subreddits,
-- 	total_submission_selftext_advs_total_links,
-- 	total_selftext_adv_submissions	

-- 	from s2_subreddit_30day_inbound_advertising_data s2
-- )

	
	select
		ys.subreddit as subreddit,
		s.creation_delta_months, 

		es.total_activity as lifetime_total_activity,
		es.total_comments as lifetime_total_comments,
		es.total_submissions as lifetime_total_submissions,
		es.unique_authors as lifetime_total_unique_authors,
	
		coalesce(s.unique_authors, 0) as unique_authors, 
		coalesce(s.total_activity, 0) as total_activity,
		coalesce(s.total_comments, 0) as total_comments,
		coalesce(s.total_submissions, 0) as total_submissions,
		coalesce(s.sub_and_comment_authors, 0) as sub_and_comment_authors,
		coalesce(s.comment_only_authors, 0) as comment_only_authors,
		coalesce(s.submission_only_authors, 0) as submission_only_authors,
		coalesce(s.total_submitters, 0) as total_submitters,
		coalesce(s.total_commenters, 0) as total_commenters,
		coalesce(s.retentive_authors, 0) as retentive_authors,
		coalesce(s.retentive_activity, 0) as retentive_activity,
		coalesce(s.non_retentive_activity, 0) as non_retentive_activity,
		coalesce(s.retentive_comments, 0) as retentive_comments,
		coalesce(s.retentive_submissions, 0) as retentive_submissions,
		coalesce(s.retentive_commenters, 0) as retentive_commenters,
		coalesce(s.retentive_submitters, 0) as retentive_submitters,
		coalesce(s.has_deleted_author, 0) as has_deleted_author,
		coalesce(s.deleted_activity, 0) as deleted_activity,
		coalesce(s.deleted_submissions, 0) as deleted_submissions,
		coalesce(s.deleted_comments, 0) as deleted_comments,

		coalesce(f6s.total_comments, 0) as first_period_total_comments,
		coalesce(f6s.total_activity, 0) as first_period_total_activity,
		coalesce(f6s.total_submissions, 0) as first_period_total_submissions,
		coalesce(f6s.unique_authors, 0) as first_period_unique_authors,





		cd0.total_activity as cd0_total_activity,
		cd0.total_comments as cd0_total_comments,
		cd0.unique_authors as cd0_unique_authors,

		cd2.total_activity as cd2_total_activity,
		cd2.total_comments as cd2_total_comments,
		cd2.unique_authors as cd2_unique_authors,
	

		cd3.total_activity as cd3_total_activity,
		cd3.total_comments as cd3_total_comments,
		cd3.unique_authors as cd3_unique_authors,

		cd6.total_activity as cd6_total_activity,
		cd6.total_comments as cd6_total_comments,
		cd6.unique_authors as cd6_unique_authors,

		cd12.total_activity as cd12_total_activity,
		cd12.total_comments as cd12_total_comments,
		cd12.unique_authors as cd12_unique_authors,


		cdminus1.total_activity as prev_total_activity,
		cdminus1.total_comments as prev_total_comments,
		cdminus1.unique_authors as prev_unique_authors,


	
		cd0.s2_avg_activity as c0_avg_activity,
		cd0.s2_avg_comments as c0_avg_comments,
		cd0.s2_avg_submissions as c0_avg_submissions,
		cd0.s2_avg_authors as c0_avg_authors,
	
		cd2.s2_avg_activity as c2_avg_activity,
		cd2.s2_avg_comments as c2_avg_comments,
		cd2.s2_avg_authors as c2_avg_authors,

		cd3.s2_avg_activity as c3_avg_activity,
		cd3.s2_avg_comments as c3_avg_comments,
		cd3.s2_avg_authors as c3_avg_authors,


		cdminus1.s2_avg_activity as prev_avg_activity,
		cdminus1.s2_avg_comments as prev_avg_comments,
		cdminus1.s2_avg_submissions as prev_avg_submissions,
		cdminus1.s2_avg_authors as prev_avg_authors,


	
		--
	    -- 2nd month snapshot
	    --


		(s2.total_activity is not null) as n2_present,
		coalesce(s2.total_activity,0) as n2_total_activity,
		coalesce(s2.unique_authors,0) as n2_unique_authors,
		coalesce(s2.total_submissions,0) as n2_total_submissions,
		coalesce(s2.total_comments,0) as n2_total_comments,

		-- 2nd month snapshot: This is the fraction of activity in the predicted month to the snapshot month
		case when cd0.total_activity > 0 then (coalesce(cd2.total_activity,0)) / (cd0.total_activity::decimal) else NULL end  as n2_total_activity_rate5,
		case when cd0.total_comments > 0 then (coalesce(cd2.total_comments,0)) / (cd0.total_comments::decimal) else NULL end  as n2_total_comments_growth_rate5,
		case when cd0.unique_authors > 0 then (coalesce(cd2.unique_authors,0)) / (cd0.unique_authors::decimal) else NULL end  as n2_unique_authors_growth_rate5,
	
	

		--
		-- N3
		--

		(s3.total_activity is not null) as n3_present,
		coalesce(s3.total_activity,0) as n3_total_activity,
		coalesce(s3.unique_authors,0) as n3_unique_authors,
		coalesce(s3.total_submissions,0) as n3_total_submissions,
		coalesce(s3.total_comments,0) as n3_total_comments,	

	
		-- this is the average cumulative growth rate of this month over the previous used in Tsugawa & Niida
		--case when cd0.s2_cum_activity > 0 then ((coalesce(cd3.s2_cum_activity,0) - coalesce(cd0.s2_cum_activity,0))/3.0) / (cd0.s2_cum_activity::decimal / (s.creation_delta_months::decimal+1) ) else NULL end  as n3_total_activity_rate,
		--case when cd0.s2_cum_comments > 0 then ((coalesce(cd3.s2_cum_comments,0) - coalesce(cd0.s2_cum_comments,0))/3.0) / (cd0.s2_cum_comments::decimal / (s.creation_delta_months::decimal+1) ) else NULL end as n3_total_comments_growth_rate,
		--case when cd0.s2_cum_unique_authors > 0 then ((coalesce(cd3.s2_cum_unique_authors,0) - coalesce(cd0.s2_cum_unique_authors,0))/3.0) / (cd0.s2_cum_unique_authors::decimal / (s.creation_delta_months::decimal+1) ) else NULL end as n3_unique_authors_growth_rate,

		--case when cd0.total_activity > 0 then ((coalesce(cd3.total_activity,0) - coalesce(cd0.total_activity,0))/3.0) / (cd0.total_activity::decimal / (s.creation_delta_months::decimal+1) ) else NULL end  as n3_total_activity_rate_a,
		--case when cd0.total_comments > 0 then ((coalesce(cd3.total_comments,0) - coalesce(cd0.total_comments,0))/3.0) / (cd0.total_comments::decimal / (s.creation_delta_months::decimal+1) ) else NULL end as n3_total_comments_growth_rate_a,
		--case when cd0.unique_authors > 0 then ((coalesce(cd3.unique_authors,0) - coalesce(cd0.unique_authors,0))/3.0) / (cd0.unique_authors::decimal / (s.creation_delta_months::decimal+1) ) else NULL end as n3_unique_authors_growth_rate_a,

		-- This is the % of new growth in the specific month with cumulative amounts (so smaller percentages except in spiky growth)
		-- NOTE: I think dividing this by the predcited month rather than the prior month is probably a mistake
		--case when cd3.s2_cum_activity > 0 then ((coalesce(cd3.s2_cum_activity,0) - coalesce(cd2.s2_cum_activity,0))) / (cd3.s2_cum_activity::decimal)  else NULL end  as n3_total_activity_rate2,
		--case when cd3.s2_cum_comments > 0 then ((coalesce(cd3.s2_cum_comments,0) - coalesce(cd2.s2_cum_comments,0))) / (cd3.s2_cum_comments::decimal)  else NULL end as n3_total_comments_growth_rate2,
		--case when cd3.s2_cum_unique_authors > 0 then ((coalesce(cd3.s2_cum_unique_authors,0) - coalesce(cd2.s2_cum_unique_authors,0))) / (cd3.s2_cum_unique_authors::decimal)  else NULL end as n3_unique_authors_growth_rate2,

		-- This is the fraction of *new* activity over the previous month (also a percent without the multiplier of 100), should allow negative numbers
		--case when cd2.total_activity > 0 then (coalesce(cd3.total_activity,0) - coalesce(cd2.total_activity,0)::decimal) / (cd2.total_activity::decimal)  else NULL end  as n3_total_activity_rate3,
		--case when cd2.total_comments > 0 then (coalesce(cd3.total_comments,0) - coalesce(cd2.total_comments,0)) / (cd2.total_comments::decimal)  else NULL end as n3_total_comments_growth_rate3,
		--case when cd2.unique_authors > 0 then (coalesce(cd3.unique_authors,0) - coalesce(cd2.unique_authors,0)) / (cd2.unique_authors::decimal)  else NULL end as n3_unique_authors_growth_rate3,	

		-- simple fraction of activity over the previous month, decreases will be between 0 and 1
		--case when cd2.total_activity > 0 then (coalesce(cd3.total_activity,0)) / (cd2.total_activity::decimal) else NULL end  as n3_total_activity_rate4,
		--case when cd2.total_comments > 0 then (coalesce(cd3.total_comments,0)) / (cd2.total_comments::decimal) else NULL end  as n3_total_comments_growth_rate4,
		--case when cd2.unique_authors > 0 then (coalesce(cd3.unique_authors,0) ) / (cd2.unique_authors::decimal) else NULL end  as n3_unique_authors_growth_rate4,	

		-- This is the fraction of activity in the predicted month to the snapshot month
		case when cd0.total_activity > 0 then (coalesce(cd3.total_activity,0)) / (cd0.total_activity::decimal) else NULL end  as n3_total_activity_rate5,
		case when cd0.total_comments > 0 then (coalesce(cd3.total_comments,0)) / (cd0.total_comments::decimal) else NULL end  as n3_total_comments_growth_rate5,
		case when cd0.unique_authors > 0 then (coalesce(cd3.unique_authors,0)) / (cd0.unique_authors::decimal) else NULL end  as n3_unique_authors_growth_rate5,


		--case when cd0.total_activity > 0 then (coalesce(cd3.total_activity,0) - cd0.total_activity::decimal) / (cd0.total_activity::decimal) else NULL end  as n3_total_activity_rate6,
		--case when cd0.total_comments > 0 then (coalesce(cd3.total_comments,0) - cd0.total_comments::decimal) / (cd0.total_comments::decimal) else NULL end  as n3_total_comments_growth_rate6,
		--case when cd0.unique_authors > 0 then (coalesce(cd3.unique_authors,0) - cd0.unique_authors::decimal) / (cd0.unique_authors::decimal) else NULL end  as n3_unique_authors_growth_rate6,


		-- This is the average growth as used in Tsugawa & Niida
		--case when (cdminus1.s2_cum_activity - cdminus2.s2_cum_activity) > 0 then ((coalesce(cd0.s2_cum_activity,0) - coalesce(cdminus1.s2_cum_activity,0)) / (cdminus1.s2_cum_activity - cdminus2.s2_cum_activity)) else NULL end as prev_total_activity_growth,
		--case when (cdminus1.s2_cum_comments - cdminus2.s2_cum_comments) > 0 then ((coalesce(cd0.s2_cum_comments,0) - coalesce(cdminus1.s2_cum_comments,0)) / (cdminus1.s2_cum_comments - cdminus2.s2_cum_comments)) else NULL end as prev_total_comments_growth,
		--case when (cdminus1.s2_cum_unique_authors - cdminus2.s2_cum_unique_authors) > 0 then ((coalesce(cd0.s2_cum_unique_authors,0) - coalesce(cdminus1.s2_cum_unique_authors,0)) / (cdminus1.s2_cum_unique_authors - cdminus2.s2_cum_unique_authors)) else NULL end as prev_unique_authors_growth,

		-- fraction of new growth in the previous month (uses cumulative, so the fraction of new growth last month cumulatively)
		-- this is likely biased because the overall cumulative should keep growing, though may work well for very spiky subreddits
		--case when cdminus1.s2_cum_activity > 0 then (coalesce(cdminus1.s2_cum_activity,0) - coalesce(cdminus2.s2_cum_activity,0)) / (cdminus1.s2_cum_activity) else NULL end as prev_total_activity_growth2,
		--case when cdminus1.s2_cum_comments > 0 then (coalesce(cdminus1.s2_cum_comments,0) - coalesce(cdminus2.s2_cum_comments,0)) / (cdminus1.s2_cum_comments) else NULL end as prev_total_comments_growth2,
		--case when cdminus1.s2_cum_unique_authors > 0 then (coalesce(cdminus1.s2_cum_unique_authors,0) - coalesce(cdminus2.s2_cum_unique_authors,0)) / (cdminus1.s2_cum_unique_authors) else NULL end as prev_unique_authors_growth2,

		-- fraction of new growth in the previous month, can be negative
		--case when cdminus2.total_activity > 0 then (coalesce(cdminus1.total_activity,0) - coalesce(cdminus2.total_activity,0))::decimal / (cdminus2.total_activity) else NULL end as prev_total_activity_growth3,
		--case when cdminus2.total_comments > 0 then (coalesce(cdminus1.total_comments,0) - coalesce(cdminus2.total_comments,0))::decimal / (cdminus2.total_comments) else NULL end as prev_total_comments_growth3,
		--case when cdminus2.unique_authors > 0 then (coalesce(cdminus1.unique_authors,0) - coalesce(cdminus2.unique_authors,0))::decimal / (cdminus2.unique_authors) else NULL end as prev_unique_authors_growth3,

		-- fraction of previous month activity relative to the prior month
		--case when cdminus2.total_activity > 0 then coalesce(cdminus1.total_activity,0)::decimal / (cdminus2.total_activity) else NULL end as prev_total_activity_growth4,
		--case when cdminus2.total_comments > 0 then coalesce(cdminus1.total_comments,0)::decimal / (cdminus2.total_comments) else NULL end as prev_total_comments_growth4,
		--case when cdminus2.unique_authors > 0 then coalesce(cdminus1.unique_authors,0)::decimal / (cdminus2.unique_authors) else NULL end as prev_unique_authors_growth4,

		-- fraction of growth this month relativeto the last month (less growth would be < 1)
		case when cdminus1.total_activity > 0 then coalesce(cd0.total_activity,0)::decimal / (cdminus1.total_activity) else NULL end as total_activity_growth,
		case when cdminus1.total_comments > 0 then coalesce(cd0.total_comments,0)::decimal / (cdminus1.total_comments) else NULL end as total_comments_growth,
		case when cdminus1.unique_authors > 0 then coalesce(cd0.unique_authors,0)::decimal / (cdminus1.unique_authors) else NULL end as unique_authors_growth,	


		--case when cdminus1.total_activity > 0 then (coalesce(cd0.total_activity,0)::decimal - cdminus1.total_activity) / (cdminus1.total_activity) else NULL end as total_activity_growth2,
		--case when cdminus1.total_comments > 0 then (coalesce(cd0.total_comments,0)::decimal - cdminus1.total_comments) / (cdminus1.total_comments) else NULL end as total_comments_growth2,
		--case when cdminus1.unique_authors > 0 then (coalesce(cd0.unique_authors,0)::decimal - cdminus1.unique_authors) / (cdminus1.unique_authors) else NULL end as unique_authors_growth2,			


		(s6.total_activity is not null) as n6_present,
		coalesce(s6.total_activity,0) as n6_total_activity,
		coalesce(s6.unique_authors,0) as n6_unique_authors,
		coalesce(s6.total_submissions,0) as n6_total_submissions,
		coalesce(s6.total_comments,0) as n6_total_comments,


		--case when cd0.s2_cum_activity > 0 then ((coalesce(cd6.s2_cum_activity,0) - coalesce(cd0.s2_cum_activity,0))/6.0) / (cd0.s2_cum_activity::decimal / (s.creation_delta_months::decimal +1) ) else NULL end  as n6_total_activity_rate,
		--case when cd0.s2_cum_comments > 0 then ((coalesce(cd6.s2_cum_comments,0) - coalesce(cd0.s2_cum_comments,0))/6.0) / (cd0.s2_cum_comments::decimal / (s.creation_delta_months::decimal +1) ) else NULL end  as n6_total_comments_growth_rate,
		--case when cd0.s2_cum_unique_authors > 0 then ((coalesce(cd6.s2_cum_unique_authors,0) - coalesce(cd0.s2_cum_unique_authors,0))/6.0) / (cd0.s2_cum_unique_authors::decimal / (s.creation_delta_months::decimal +1) ) else NULL end  as n6_unique_authors_growth_rate,

		--case when cd6.s2_cum_activity > 0 then ((coalesce(cd6.s2_cum_activity,0) - coalesce(cd5.s2_cum_activity,0))) / (cd6.s2_cum_activity::decimal) else NULL end  as n6_total_activity_rate2,
		--case when cd6.s2_cum_comments > 0 then ((coalesce(cd6.s2_cum_comments,0) - coalesce(cd5.s2_cum_comments,0))) / (cd6.s2_cum_comments::decimal) else NULL end  as n6_total_comments_growth_rate2,
		--case when cd6.s2_cum_unique_authors > 0 then ((coalesce(cd6.s2_cum_unique_authors,0) - coalesce(cd5.s2_cum_unique_authors,0))) / (cd6.s2_cum_unique_authors::decimal ) else NULL end  as n6_unique_authors_growth_rate2,

		--case when cd5.total_activity > 0 then (coalesce(cd6.total_activity,0) - coalesce(cd5.total_activity,0)::decimal) / (cd5.total_activity::decimal)  else NULL end  as n6_total_activity_rate3,
		--case when cd5.total_comments > 0 then (coalesce(cd6.total_comments,0) - coalesce(cd5.total_comments,0)::decimal) / (cd5.total_comments::decimal)  else NULL end as n6_total_comments_growth_rate3,
		--case when cd5.unique_authors > 0 then (coalesce(cd6.unique_authors,0) - coalesce(cd5.unique_authors,0)::decimal) / (cd5.unique_authors::decimal)  else NULL end as n6_unique_authors_growth_rate3,	

		--case when cd5.total_activity > 0 then (coalesce(cd6.total_activity,0)::decimal) / (cd5.total_activity::decimal) else NULL end  as n6_total_activity_rate4,
		--case when cd5.total_comments > 0 then (coalesce(cd6.total_comments,0)::decimal) / (cd5.total_comments::decimal) else NULL end  as n6_total_comments_growth_rate4,
		--case when cd5.unique_authors > 0 then (coalesce(cd6.unique_authors,0)::decimal ) / (cd5.unique_authors::decimal) else NULL end  as n6_unique_authors_growth_rate4,


		case when cd0.total_activity > 0 then (coalesce(cd6.total_activity,0)::decimal) / (cd0.total_activity::decimal) else NULL end  as n6_total_activity_rate5,
		case when cd0.total_comments > 0 then (coalesce(cd6.total_comments,0)::decimal) / (cd0.total_comments::decimal) else NULL end  as n6_total_comments_growth_rate5,
		case when cd0.unique_authors > 0 then (coalesce(cd6.unique_authors,0)::decimal ) / (cd0.unique_authors::decimal) else NULL end  as n6_unique_authors_growth_rate5,
	

		--case when cd0.total_activity > 0 then (coalesce(cd6.total_activity,0)::decimal - cd0.total_activity::decimal) / (cd0.total_activity::decimal) else NULL end  as n6_total_activity_rate6,
		--case when cd0.total_comments > 0 then (coalesce(cd6.total_comments,0)::decimal - cd0.total_comments::decimal) / (cd0.total_comments::decimal) else NULL end  as n6_total_comments_growth_rate6,
		--case when cd0.unique_authors > 0 then (coalesce(cd6.unique_authors,0)::decimal - cd0.unique_authors::decimal) / (cd0.unique_authors::decimal) else NULL end  as n6_unique_authors_growth_rate6,

		--case when (cd5.s2_cum_activity-cd4.s2_cum_activity) > 0 then ((cd6.s2_cum_activity - cd5.s2_cum_activity) / (cd5.s2_cum_activity - cd4.s2_cum_activity)) else 0 end  as n6_prev_total_activity_growth,
		--case when (cd5.s2_cum_comments-cd4.s2_cum_comments) > 0 then ((cd6.s2_cum_comments - cd5.s2_cum_comments) / (cd5.s2_cum_comments - cd4.s2_cum_comments)) else 0 end  as n6_prev_total_comments_growth,
		--case when (cd5.unique_authors-cd4.unique_authors) > 0 then ((cd6.s2_cum_unique_authors - cd5.s2_cum_unique_authors) / (cd5.s2_cum_unique_authors - cd4.s2_cum_unique_authors)) else 0 end as n6_prev_unique_authors_growth,



		coalesce(fd.num_fostering_authors, 0) as num_fostering_authors,
		coalesce(fd.total_fostering_activity, 0) as total_fostering_activity,
		coalesce(fd.total_fostering_submissions, 0) as total_fostering_submissions,
		coalesce(fd.total_fostering_comments, 0) as total_fostering_comments,
		coalesce(fd.num_fostering_mods, 0) as num_fostering_mods,

		avg(coalesce(fd.num_fostering_authors, 0)) over w as avg_fostering_authors,
		coalesce(sum(fd.total_fostering_activity) over w,0) as fostering_cumsum_total_activity,
		coalesce(sum(fd.total_fostering_submissions) over w,0) as fostering_cumsum_total_submissions,

		sum(coalesce(fd.total_fostering_submissions,0)) over w as cumsum_fostering_submissions,
		avg(coalesce(fd.total_fostering_submissions,0)) over w as avg_fostering_submissions,
		sum(coalesce(fd.total_fostering_comments,0)) over w as cumsum_fostering_comments,
		avg(coalesce(fd.total_fostering_comments,0)) over w as avg_fostering_comments,

	
		coalesce(fd.total_moderator_fostering_activity, 0) as total_moderator_fostering_activity,
		coalesce(fd.total_moderator_fostering_submissions, 0) as total_moderator_fostering_submissions,
		coalesce(fd.total_moderator_fostering_comments, 0) as total_moderator_fostering_comments,
		coalesce(fd.num_fostering_creators, 0) as num_fostering_creators,
		coalesce(fd.total_creator_fostering_activity, 0) as total_creator_fostering_activity,
		coalesce(fd.total_creator_fostering_submissions, 0) as total_creator_fostering_submissions,
		coalesce(fd.total_creator_fostering_comments, 0) as total_creator_fostering_comments,

		case when cam.num_moderators is not null and cam.num_moderators > 0 then 1 else 0 end as sub_has_mod_data,
		case when cam.num_creators is not null and cam.num_creators > 0 then 1 else 0 end as sub_has_creator_data,
	
		coalesce(sd.num_seed_authors, 0) as num_seed_authors, 
		coalesce(sd.num_seeded_content, 0) as num_seeded_content, 
		coalesce(sd.num_seeding_mods, 0) as num_seeding_mods, 
		coalesce(sd.creator_seeding, 0) as creator_seeding,

		--coalesce(ac.num_comment_adv_authors,0) as num_comment_adv_authors,
		--coalesce(ac.num_comment_adv_subreddits,0) as num_comment_adv_subreddits,
		--coalesce(ac.total_comment_adv_links, 0) as total_comment_adv_links,

		coalesce(iad.num_comment_adv_authors, 0) as num_comment_adv_authors,
		coalesce(iad.num_comment_adv_subreddits, 0) as num_comment_adv_subreddits,
		coalesce(iad.total_comment_adv_links_or_mentions, 0) as total_comment_adv_links_or_mentions,
		coalesce(iad.total_comment_adv_comments, 0) as total_comment_adv_comments,

		coalesce(iad.comment_adv_link_comments, 0) as total_comment_adv_link_comments,
		coalesce(iad.comment_adv_mention_comments, 0) as total_comment_adv_mention_comments,

		coalesce(iad.num_submission_adv_subreddits, 0) as num_submission_adv_subreddits,
		coalesce(iad.num_submission_adv_authors, 0) as num_submission_adv_authors,
		coalesce(total_submission_adv_submissions, 0) as total_submission_adv_submissions,
		coalesce(num_submission_adv_non_deleted_authors, 0) as num_submission_adv_non_deleted_authors,
	
		coalesce(iad.submission_adv_distinct_title_authors, 0) as num_submission_title_adv_authors,
		coalesce(iad.submission_adv_distinct_title_subreddits, 0) as num_submission_title_adv_subreddits,
		coalesce(iad.total_submission_adv_title_links_and_mentions, 0) as total_submission_title_advs_total_links,
		coalesce(iad.submission_adv_distinct_title_submissions, 0) as total_title_adv_submissions,
		
		coalesce(iad.submission_adv_distinct_selftext_authors, 0) as num_submission_selftext_adv_authors,
		coalesce(iad.submission_adv_distinct_selftext_subreddits, 0) as num_submission_selftext_adv_subreddits,
		coalesce(iad.total_submission_adv_selftext_links_and_mentions, 0) as total_submission_selftext_advs_total_links,
		coalesce(iad.submission_adv_distinct_selftext_submissions, 0) as total_selftext_adv_submissions,

		coalesce(iad.num_crosspost_authors, 0) as num_crosspost_authors,
		coalesce(iad.num_crosspost_nondeleted_authors, 0) as num_crosspost_nondeleted_authors,
		coalesce(iad.num_crosspost_subreddits, 0) as num_crosspost_subreddits,
		coalesce(iad.num_crosspost_submissions, 0) as num_crosspost_submissions,



		sum(coalesce(iad.total_submission_adv_submissions,0)) over w as cumsum_adv_submissions,
		avg(coalesce(iad.total_submission_adv_submissions,0)) over w as avg_adv_submissions,
		sum(coalesce(iad.total_comment_adv_comments,0)) over w as cumsum_adv_comments,
		avg(coalesce(iad.total_comment_adv_comments,0)) over w as avg_adv_comments,
		sum(coalesce(iad.num_crosspost_submissions,0)) over w as cumsum_adv_crosspost_submissions,
		avg(coalesce(iad.num_crosspost_submissions,0)) over w as avg_adv_crosspost_submissions,


		case when s.creation_delta_months = 0 then 0 else coalesce(fm.first_month_total_activity, 0) end as first_month_total_activity,
		case when s.creation_delta_months = 0 then 0 else coalesce(fm.first_month_total_submissions, 0) end as first_month_total_submissions,
		case when s.creation_delta_months = 0 then 0 else coalesce(fm.first_month_total_comments, 0) end as first_month_total_comments,
		case when s.creation_delta_months = 0 then 0 else coalesce(fm.first_month_unique_authors,0) end as first_month_unique_authors,
		coalesce(fm.first_month_activity_gini, 0) as first_month_activity_gini,
		coalesce(fm.first_month_comments_gini, 0) as first_month_comments_gini,
		coalesce(fm.first_month_submissions_gini, 0) as first_month_submissions_gini,	

		coalesce(cd0.s2_cum_activity, 0) as cumsum_total_activity,
		coalesce(cd0.s2_cum_submissions, 0) as cumsum_total_submissions,
		coalesce(cd0.s2_cum_comments, 0) as cumsum_total_comments,


		coalesce(sg30.total_activity_gini,0) as total_activity_gini,
		coalesce(sg30.total_comments_gini,0) as total_comments_gini,
		coalesce(sg30.total_submissions_gini,0) as total_submissions_gini,

		l3a.last_3_months as active_users_2years,
		l3a.total_unique_authors as total_unique_authors_2years,
		l3a.pct_active_last_3mo as pct_active_last_3mo_2years,

		s30r.retention_rate,
		s30r.turnover_rate,

		coalesce(ssem.num_pos_engaged, 0) as num_submissions_pos_engaged,
		coalesce(ssem.num_pos_engaged_no_automod, 0) as num_submissions_pos_engaged_no_automod,
		coalesce(ssem.num_any_engaged, 0) as num_submissions_any_engaged,
		coalesce(ssem.has_comments, 0) as num_submissions_with_comments,
		case when coalesce(ssem.total_submissions,0) = 0 then 0 else coalesce(ssem.has_comments, 0)::decimal / ssem.total_submissions::decimal end as pct_submissions_with_comments,
		case when coalesce(ssem.total_submissions, 0) = 0 then 0 else coalesce(ssem.num_pos_engaged, 0)::decimal / ssem.total_submissions::decimal end as pct_submissions_with_engagement,
		case when coalesce(ssem.total_submissions, 0) = 0 then 0 else coalesce(ssem.num_pos_engaged_no_automod::decimal, 0) / ssem.total_submissions::decimal end as pct_submissions_with_engagement_no_automod,


	m12suc.total_activity as n12_total_activity,
	m12suc.total_submissions as n12_total_submissions,
	m12suc.total_comments as n12_total_comments,
	m12suc.unique_authors as n12_unique_authors,


	m12suc.avg_authors as n12_avg_authors,

	m12suc.num_12m_active_months as n12_num_active_months,
	m12suc.avg_comments as n12_avg_comments,
	m12suc.cumsum_comments as n12_cumsum_comments,

	m12suc.avg_submissions as n12_avg_submissions,
	m12suc.cumsum_submissions as n12_cumsum_submissions,	

	m12suc.avg_activity_gini as n12_avg_activity_gini,
	m12suc.avg_comments_gini as n12_avg_comments_gini,

	m12suc.avg_submissions_gini as n12_avg_submissions_gini,


	m12suc.avg_retention_rate as n12_avg_retention_rate,

	m12suc.num_m12_user_joins as n12_count_new_users
		
		

	into s2_subreddit_monthly_data_combined
	
	from year_subs ys 
	left join first_month fm on fm.subreddit = ys.subreddit
	left join eligible_subs es on es.name = ys.subreddit
	left join s2_subreddit_30day_activity_summary s on s.subreddit = ys.subreddit
	left join s2_subreddit_30day_activity_summary s2 on s2.subreddit = ys.subreddit and s2.creation_delta_months = s.creation_delta_months+2
	left join s2_subreddit_30day_activity_summary s3 on s3.subreddit = ys.subreddit and s3.creation_delta_months = s.creation_delta_months+3
	left join s2_subreddit_30day_activity_summary s6 on s6.subreddit = ys.subreddit and s6.creation_delta_months = s.creation_delta_months+6
	left join cumulative_data cd0 on cd0.subreddit = ys.subreddit and cd0.creation_delta_months = s.creation_delta_months
	left join cumulative_data cdminus1 on cdminus1.subreddit = ys.subreddit and cdminus1.creation_delta_months = s.creation_delta_months-1
	left join cumulative_data cdminus2 on cdminus2.subreddit = ys.subreddit and cdminus2.creation_delta_months = s.creation_delta_months-2
	--left join cumulative_data cd1 on cd1.subreddit = fm.subreddit and cd1.creation_delta_months = s.creation_delta_months+1
	left join cumulative_data cd2 on cd2.subreddit = ys.subreddit and cd2.creation_delta_months = s.creation_delta_months+2
	left join cumulative_data cd3 on cd3.subreddit = ys.subreddit and cd3.creation_delta_months = s.creation_delta_months+3
	--left join cumulative_data cd4 on cd4.subreddit = fm.subreddit and cd4.creation_delta_months = s.creation_delta_months+4
	left join cumulative_data cd5 on cd5.subreddit = ys.subreddit and cd5.creation_delta_months = s.creation_delta_months+5
	left join cumulative_data cd6 on cd6.subreddit = ys.subreddit and cd6.creation_delta_months = s.creation_delta_months+6
	left join cumulative_data cd12 on cd12.subreddit = ys.subreddit and cd12.creation_delta_months = s.creation_delta_months+12	
	left join fostering_data fd on fd.subreddit = ys.subreddit and fd.creation_delta_months = s.creation_delta_months
	left join seeding_content sd on sd.subreddit = ys.subreddit
	left join creator_and_mod_subs cam on cam.subreddit = ys.subreddit
	--left join advertising_comments ac on ac.subreddit = fm.subreddit and ac.creation_delta_months = s.creation_delta_months
	left join s2_subreddit_30day_inbound_advertising_data iad on ys.subreddit = iad.mentioned_sub_name and s.creation_delta_months = iad.creation_delta_months
	left join s2_subreddit_gini_30days sg30 on sg30.subreddit = ys.subreddit and sg30.creation_delta_months = s.creation_delta_months
	left join first_6mo_sums f6s on ys.subreddit = f6s.subreddit
	left join s2_last_3month_activity_2years l3a on l3a.subreddit = ys.subreddit
	left join subreddit_30day_retention_fixed s30r on s30r.subreddit = ys.subreddit and s30r.creation_delta_months = s.creation_delta_months
	left join s2_submissions_engagement_monthly ssem on ssem.subreddit = ys.subreddit and ssem.creation_delta_months = s.creation_delta_months
	left join s2_m12_success_metrics m12suc on m12suc.subreddit = ys.subreddit and m12suc.creation_delta_months = s.creation_delta_months+12
	
	window w as (partition by fm.subreddit order by s.creation_delta_months asc);
	;


grant select on s2_subreddit_monthly_data_combined to public;


create index on s2_subreddit_monthly_data_combined(subreddit);
create index on s2_subreddit_monthly_data_combined(creation_delta_months);

with comment_counts as (
	select article, 
		count(*) as num_comments,
		count(distinct author) as unique_authors,
		max(depth) as tree_depth,
		min((data->>'score')::int) as min_score, 
		max((data->>'score')::int) as max_score,
		sum(case when (data->>'score')::int < 0 then 1 else 0 end) as num_comments_neg_score,
		sum(case when (data->>'score')::int = 0 then 1 else 0 end) as num_comments_zero_score,
		sum(case when (data->>'score')::int = 1 then 1 else 0 end) as num_comments_one_score,
		sum(case when (data->>'score')::int > 1 then 1 else 0 end) as num_comments_gt_1,
		min(created_utc) as first_comment_date_utc,
		max(created_utc) as last_comment_date_utc,
		extract(epoch from (max(created_utc) - min(created_utc))) as comment_thread_life
	from s1_coding_comments
	group by article
)
select s.subreddit, s.id, created_utc, s.data->>'score' as score, s.data->>'num_comments' as num_comments1, cc.*
	from submissions s
	left join comment_counts cc on cc.article = s.id
	where subreddit in (
		'AskWomenOver30','WomensSoccer','UpliftingNews','EarthScience',
		'FZero','LinusTechTips','AnimalsFailing','Eskrima'
	)
	








select subreddit, id, created_utc, depth, data->>'id' as idb36, data->>'link_id'  from s1_coding_comments where depth > 100
order by depth asc;

-- select depth, count(*) from s1_coding_comments
-- group by depth;











with recursive cte as (
		select id, parent, depth
			from s1_coding_comments
			where depth = 1
	union all
		select s.id, s.parent, c.depth+1
		from cte c
		join s1_coding_comments s on s.parent = c.id
		where c.depth < 10000
)
update s1_coding_comments s set depth = cte.depth from cte where cte.id = s.id and cte.depth > 1







update  s1_coding_comments as s set s.depth = NULL where s.depth = 0








select subreddit, count(*) from submissions
where subreddit in ('MonkeyIsland', 'Fzero')
group by subreddit;















with 
subreddit_list as (
	select name from subreddit_summary
	where total_submissions >= 100000
),
raw_data as (
	select subreddit, 
		extract(year from date) as year,  
		author,
		num_submissions
	from subreddit_list sl
	left join user_subreddit_daily_summary usds on usds.subreddit = sl.name
	where author != '[deleted]' and num_submissions != 0
),
summarized as (
	select subreddit, year, author, sum(num_submissions) as num_submissions
	from raw_data
	group by subreddit, year, author
	order by num_submissions asc
),
ranked_data as (
	select 
		subreddit, 
		year,
		author,
		num_submissions, 
		row_number() over (partition by subreddit,year order by num_submissions) as rank
	from summarized
	where num_submissions > 0
	order by rank asc
)
select
	subreddit,
	year,
	sum(num_submissions) as total_submissions,
	count(distinct author) as unique_authors,
	(2.0 * sum(num_submissions * rank )/(count(*)::decimal * sum(num_submissions)))-1.0 - (1.0/count(*)),
	((2.0 * sum(num_submissions * rank )/sum(num_submissions)) - (count(*)+1.0))/count(*)	
from ranked_data
group by subreddit, year
order by subreddit, year


--select
--	subreddit, year, (2.0 * sum(num_submissions * rank )/(count(*)::decimal * sum(num_submissions)))-1.0 - (1.0/count(*))

--	(2.0 * sum(num_submissions * rank )/(count(*)::decimal * sum(num_submissions)))-1.0 - (1.0/count(*)),
--	((2.0 * sum(num_submissions * rank )/sum(num_submissions)) - (count(*)+1.0))/count(*)
--from ranked_data
--group by subreddit, year
--order by year, subreddit

--	count(*) as cnt,
--  sum(num_submissions),
--	sum(num_submissions*rank) as g1,
-- 	count(*)::decimal * sum(num_submissions)::decimal as g2,


--select subreddit, year, author, num_comments, rank
--from ranked_data
--limit 1000;
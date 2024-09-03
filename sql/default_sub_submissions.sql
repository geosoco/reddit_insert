
--
-- create default_sub_submissions table with only submissions from default subs
--

drop table if exists default_sub_submissions;

select
	subreddit,
	id,
	author,
	created_utc,
	date_trunc('month', created_utc) as month,
	(data->>'score')::int as score,
	(data->>'num_comments')::int as num_comments
into default_sub_submissions
from submissions
where subreddit in (
'AdviceAnimals', 'announcements', 'Art', 'AskReddit', 'askscience', 'atheism', 'aww', 'bestof', 'blog', 'books', 'creepy', 'dataisbeautiful', 'DIY', 'Documentaries', 'EarthPorn', 'explainlikeimfive', 'Fitness', 'food', 'funny', 'Futurology', 'gadgets', 'gaming', 'GetMotivated', 'gifs', 'history', 'IAmA', 'InternetIsBeautiful', 'Jokes', 'LifeProTips', 'listentothis', 'mildlyinteresting', 'movies', 'Music', 'news', 'nosleep', 'nottheonion', 'OldSchoolCool', 'personalfinance', 'philosophy', 'photoshopbattles', 'pics', 'politics', 'science', 'Showerthoughts', 'space', 'sports', 'technology', 'television', 'tifu', 'todayilearned', 'TwoXChromosomes', 'UpliftingNews', 'videos', 'worldnews', 'WritingPrompts', 'WTF'
);

create index on default_sub_submissions(author);
create index on default_sub_submissions(created_utc);
create index on default_sub_submissions(id);
create index on default_sub_submissions(month);
create index on default_sub_submissions(subreddit);

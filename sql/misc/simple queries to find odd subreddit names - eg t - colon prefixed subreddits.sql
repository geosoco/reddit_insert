--
-- In 2012, reddit had about a 24 hour period where they allowed subreddits to be created with a prefix
-- of 't:', where the colon is not traditionally an allowed character for users to engage.
--
-- The platform has since used the colon to move inactive subreddits to other names, commonly with the 'a:<id>'
-- prefix
--
-- It's unclear why this was done, but I *believe* (need to verify) the colon breaks the
-- regex for identifying subreddits, which makes them difficult to run some analyses on regarding that
--


--
-- Find the t:<subname> subreddits in 
-- 

select *
from s2_subreddit_monthly_data_combined
where subreddit ~ '^t:.*' and creation_delta_months = 0



--
--
--

select * from subreddits where display_name ~ '^t:.*'


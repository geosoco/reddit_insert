--
-- s1_comment_sub_link_details_all_subs
--
-- this table has all mentions and links of the selected subreddits, so includes data outside these subreddits
--
-- req: this depends on s2_comment_sub_link_details to be created first, which

drop table if exists s1_comment_sub_link_details_all_subs;

select * 
into s1_comment_sub_link_details_all_subs
from s2_comment_sub_link_details 
where
lower(mentioned_sub_name) in ('animalsfailing', 'fzero', 'earthscience', 'eskrima', 'linustechtips', 'womenssoccer', 'askwomenover30', 'upliftingnews');


grant select on s1_comment_sub_link_details_all_subs to public;




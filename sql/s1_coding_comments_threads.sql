drop table if exists s1_coding_comments_threads;

with recursive
simple_comments as (
	select article, id, author, parent as parent, 
		case when parent is null then 1 else NULL end as depth
	from s1_coding_comments
),
descendants (article, id, author, root_comment, parent, depth) as (
	select article, id, c.author, id as root_comment, parent, c.depth
		from simple_comments c 
		where parent is null
	union all
	select d.article, c.id, c.author, d.root_comment, c.parent, d.depth+1 as depth
		from simple_comments c
		join descendants d on d.id = c.parent
		where c.depth is null
)
select * 
into s1_coding_comments_threads
from descendants
order by article


grant select on s1_coding_comments_threads to public;

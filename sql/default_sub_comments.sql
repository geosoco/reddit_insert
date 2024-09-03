drop table if exists default_sub_comments;


create table default_sub_comments(
    id bigint,
    subreddit text,
    created_utc timestamp without time zone,
    author text,
    article bigint,
    parent bigint,
    toplevel_comment bigint,
    
    length int,
    isdeleted bool,
    isremoved bool,
    isdeletedauthor bool,
    score int,
    depth int,
    num_children int,
    sum_child_score int
);


insert into default_sub_comments
(

select
    c.id, 
	dsm.subreddit,
	c.created_utc,
	c.author,
    c.article,
    (c.parent)::bigint as parent,
    (case when c.parent is null then c.id  else null end)::bigint as toplevel_comment,
    
    length(data->>'body') as length,
    (data->>'body' = '[deleted]') as isdeleted,
    (data->>'body' = '[removed]') as isremoved,
    (author = '[deleted]') as isdeletedauthor,
    (data->>'score')::integer as score, 
    (case when c.parent is null then 0 else NULL end)::int as depth,
    null::int as num_children,
    null::int as sum_child_score
    
    
--	into default_sub_comments
	from default_subreddit_meta dsm
	left join comments c on dsm.subreddit = c.subreddit
	where
	
	dsm.included = 'yes'
	and age(date_trunc('month', created_utc)::timestamp without time zone, date_trunc('month', dsm.added)) >= interval '-24 month'
	and age(date_trunc('month', created_utc)::timestamp without time zone, date_trunc('month', dsm.added)) < interval '25 month'
  );



vacuum freeze analyze;

grant select on default_sub_comments to public;

alter table default_sub_comments add primary key (id);
create index on default_sub_comments(subreddit);
create index on default_sub_comments(author);
create index on default_sub_comments(article);
create index on default_sub_comments(created_utc);
create index on default_sub_comments(parent);
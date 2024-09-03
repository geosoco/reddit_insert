drop table if exists s1_coding_comments;
CREATE TABLE IF NOT EXISTS public.s1_coding_comments
(
    id bigint NOT NULL,
    created_utc timestamp without time zone,
    article bigint,
    subreddit_id integer,
    parent bigint nullable,
	depth integer nullable,
    author text COLLATE pg_catalog."default",
    subreddit text COLLATE pg_catalog."default",
    data jsonb
);

insert into s1_coding_comments
select
	id, created_utc, article, subreddit_id, parent, NULL as depth, author, subreddit, data
from comments
where subreddit in (
'AskWomenOver30','WomensSoccer','UpliftingNews','EarthScience','Fzero','LinusTechTips','AnimalsFailing','Eskrima'
)


alter table s1_coding_comments add primary key (id);


CREATE INDEX s1_coding_comments_author_idx
    ON public.s1_coding_comments USING btree
    (author COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX s1_coding_comments_created_utc_brin_idx
    ON public.s1_coding_comments USING brin
    (created_utc)
    TABLESPACE pg_default;

CREATE INDEX s1_coding_comments_created_utc_idx
    ON public.s1_coding_comments USING btree
    (created_utc ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX s1_coding_comments_id_brin_idx
    ON public.s1_coding_comments USING brin
    (id)
    TABLESPACE pg_default;


CREATE INDEX s1_coding_comments_subreddit_idx
    ON public.s1_coding_comments USING btree
    (subreddit COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;



update s1_coding_comments set depth = 1 where parent is null;


with recursive cte as (
        select id, parent, depth
            from s1_coding_comments
            where depth = 1
    union all
        select s.id, s.parent, c.depth+1
        from cte c
        join s1_coding_comments s on s.parent = c.id
        where c.depth < 100
)
update s1_coding_comments s set depth = cte.depth from cte where cte.id = s.id and cte.depth > 1
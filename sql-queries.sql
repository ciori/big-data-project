-- List of SQL queries used for the project

-- schema: public, table: users_keywords

-- users_keywords CREATION SQL (PostgreSQL)
CREATE TABLE public.users_keywords 
	user_id int4 NOT NULL,
	keywords varchar NULL
);
CREATE INDEX users_keywords_user_id_idx ON public.users_keywords USING btree (user_id);

-- query to get the users ids of a range of users (used to obtain min and max id)
select min(user_id) as min, max(user_id) as max 
from (select distinct user_id 
      from public.users_keywords 
      order by user_id 
      offset <offset> 
      limit <users_per_iteration>) as a;

-- query to get the keywords of a single interval of users (given min and max id of the interval)
select * 
from public.users_keywords 
where user_id>='<min-id>' and user_id<='<max-id>';
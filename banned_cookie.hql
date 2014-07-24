use ayang;

drop table if exists ay_temp_butterfly_cookie;
create table ay_temp_butterfly_cookie as
select event_date, event_type, cookie, count(*) ct
from ay_temp_butterfly_pq
where cookie <> 'undefined' and cookie <> '' and cookie is not NULL
and event_type IN ('AD_PLAY', 'WIDGET_CREATED', 'VIDEO_CONTENT_BEGIN')
group by event_date, event_type, cookie
;

drop table if exists ay_temp_butterfly_cookie_sum;
create table ay_temp_butterfly_cookie_sum as
select event_date, event_type, avg(ct) avg_ct, stddev(ct) stddev_ct
from ay_temp_butterfly_cookie
group by event_date, event_type
;

drop table if exists ay_temp_butterfly_banned_cookie;
create table ay_temp_butterfly_banned_cookie as
select a.cookie, '10 stddev' note, a.event_date
from 
	ay_temp_butterfly_cookie a
	join
	ay_temp_butterfly_cookie_sum b
on (a.event_type = b.event_type and a.event_date = b.event_date)
	left outer join (select cookie from banned_cookie_butterfly where note = '10 stddev') c
	on (a.cookie = c.cookie)
where a.ct > b.avg_ct + 10*b.stddev_ct
--safety
and a.ct > 30
and c.cookie is NULL
;

insert into table banned_cookie_butterfly
select cookie, note,
	max(event_date) event_date
from ay_temp_butterfly_banned_cookie
group by cookie, note
;
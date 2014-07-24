use ayang;

drop table if exists ay_temp_butterfly_cookie_ip;
create table ay_temp_butterfly_cookie_ip as
select distinct cookie, ip, event_date from 
(
select event_date, time_local, event_type, cookie, ip, count(*) ct
from ay_temp_butterfly_pq
where cookie <> 'undefined' and cookie <> '' and cookie is not NULL
and event_type IN ('AD_PLAY', 'WIDGET_CREATED', 'VIDEO_CONTENT_BEGIN')
group by event_date, time_local, event_type, cookie, ip
having count(*) >= 5
) t
;

drop table if exists ay_temp_butterfly_banned_cookie_ip;
create table ay_temp_butterfly_banned_cookie_ip as
select a.cookie, a.ip, '5 views per second' note, a.event_date
from 
	ay_temp_butterfly_cookie_ip a
	left outer join (select cookie, ip, note from banned_cookie_ip_butterfly where note = '5 views per second') b
	on (a.cookie = b.cookie and a.ip = b.ip)
where b.note is NULL
;

insert into table banned_cookie_ip_butterfly
select cookie, ip, note, max(event_date) event_date
from ay_temp_butterfly_banned_cookie_ip
group by cookie, ip, note
;
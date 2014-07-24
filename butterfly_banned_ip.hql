use ayang;

drop table if exists ay_temp_butterfly_ip;
create table ay_temp_butterfly_ip as
select event_date, event_type, ip, count(*) ct
from ay_temp_butterfly_pq
where event_type IN ('AD_PLAY', 'WIDGET_CREATED', 'VIDEO_CONTENT_BEGIN')
group by event_date, event_type, ip
;

drop table if exists ay_temp_butterfly_ip_sum;
create table ay_temp_butterfly_ip_sum as
select event_date, event_type, avg(ct) avg_ct, stddev(ct) stddev_ct
from ay_temp_butterfly_ip
group by event_date, event_type
;

drop table if exists ay_temp_butterfly_banned_ip;
create table ay_temp_butterfly_banned_ip as
select a.ip, '10 stddev' note, a.event_date
from 
	ay_temp_butterfly_ip a
	join
	ay_temp_butterfly_ip_sum b
on (a.event_type = b.event_type and a.event_date = b.event_date)
	left outer join (select ip, event_date, note from banned_ip_butterfly where note = '10 stddev') c
	on (a.ip = c.ip and a.event_date = c.event_date)
where a.ct > b.avg_ct + 10*b.stddev_ct
--safety
and a.ct > 100
and c.note is NULL
;

insert into table banned_ip_butterfly
select ip, note, max(event_date) event_date
from ay_temp_butterfly_banned_ip
group by ip, note;

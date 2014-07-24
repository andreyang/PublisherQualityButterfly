use ayang;

drop table if exists ay_temp_butterfly_site_ip;
create table ay_temp_butterfly_site_ip as
select distinct event_date, ip, p.domain_id
from ay_temp_butterfly_pq a join client_portal.players p on (a.player_id = p.uuid)
;

drop table if exists overlap_ip_sum_temp_butterfly;
create table overlap_ip_sum_temp_butterfly as
select a.event_date, a.domain_id, count(distinct a.ip) ip_ct, count(distinct case when a.domain_id = b.domain_id then NULL else b.ip end) overlap_ip_ct
from ay_temp_butterfly_site_ip a left outer join ay_temp_butterfly_site_ip b
on (a.event_date = b.event_date and a.ip = b.ip)
group by a.event_date, a.domain_id
;
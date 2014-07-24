use ayang;

drop table if exists publisher_quality_sum_temp_butterfly;
create table publisher_quality_sum_temp_butterfly as
select coalesce(p.domain_id,0) domain_id,
        a.event_date,
	case when COALESCE(ci.note, c.note, i.note) is not NULL then 1 else 0 end is_bot,
	--skip records for cookie/ip comparison
	count(distinct case when a.cookie <> '' and a.cookie <> 'undefined' then a.ip else NULL end) ip_ct_neat, 
	count(distinct case when a.cookie <> '' and a.cookie <> 'undefined' then a.cookie else NULL end) cookie_ct_neat,
	sum(case when a.cookie <> '' and a.cookie <> 'undefined' and upper(a.event_type) = 'VIDEO_CONTENT_BEGIN' then 1 else 0 end) stream_ct_neat,
	sum(case when a.cookie <> '' and a.cookie <> 'undefined' and a.event_type = 'AD_PLAY' then 1 else 0 end) ad_ct_neat,
	count(distinct case when a.cookie <> '' and a.cookie <> 'undefined' and upper(a.event_type) = 'WIDGET_CREATED' then a.player_instance else NULL end) player_ct_neat,
	--player stats
	count(distinct a.cookie) cookie_ct,
	count(distinct a.ip) ip_ct,
	count(distinct case when upper(a.event_type) = 'WIDGET_CREATED' then a.player_instance else NULL end) player_ct,
	sum(case when upper(a.event_type) = 'WIDGET_CREATED' then 1 else 0 end) impression,
	count(distinct case when upper(a.event_type) = 'VIDEO_CONTENT_BEGIN' then a.player_instance else NULL end) clicked_player_ct,
	count(distinct case when upper(a.event_type) = 'VOLUME_CHANGE' then a.player_instance else NULL end) player_adjusted_volume_ct,
	count(distinct case when upper(a.event_type) = 'MUTED' then a.player_instance else NULL end) player_muted_ct,
	0 player_adNotEnabled_ct, --count(distinct case when a.event_type = 'adNotEnabled' then a.player_instance else NULL end) player_adNotEnabled_ct,
	--work around an config issue with overlay ads
	0 player_adAbsent_ct,--count(distinct case when a.event_type = 'SLOT_ABSENT' then a.player_instance else NULL end) player_adAbsent_ct,
	0 player_in_iframe_ct, --count(distinct case when upper(a.event_type) = 'WIDGET_CREATED' and a.rg_iframe = 'true' then a.player_instance else NULL end) player_in_iframe_ct,
	0 player_visible_ct, --count(distinct case when upper(a.event_type) = 'WIDGET_CREATED' and a.rg_visible IN ('100%25', '%3E50%25') then a.player_instance else NULL end) player_visible_ct,
	--stream stats
	sum(case when upper(a.event_type) = 'VIDEO_CONTENT_BEGIN' then 1 else 0 end) stream_ct,
	sum(case when upper(a.event_type) = 'VOLUME_CHANGE' and a.media_volume = '0' then 1 else 0 end) stream_0_volume_ct,
	sum(case when upper(a.event_type) = 'INFO' and upper(a.auto_play) = 'TRUE' then 1 else 0 end) stream_autostart_ct,
	--ads stats
	sum(case when a.event_type = 'AD_PLAY' then 1 else 0 end) ad_ct,
	sum(case when a.event_type = 'AD_CLICK' then 1 else 0 end) adClicked2Site,
	0 adSkipped, --sum(case when a.event_type = 'adSkipped' then 1 else 0 end) adSkipped,
	sum(case when a.event_type = 'SLOT_STOPPED' then 1 else 0 end) adsStopped,
	0 adError --sum(case when a.event_type = 'adError' then 1 else 0 end) adError
from ay_temp_butterfly_pq a
	left outer join client_portal.players p on (a.player_id = p.uuid)
	left outer join (select cookie, ip, CONCAT('T1 - ', note) note from banned_cookie_ip_butterfly) ci
	on (a.cookie = ci.cookie and a.ip = ci.ip)
	left outer join (select ip, event_date, CONCAT('T3 ip - ', note) note from banned_ip_butterfly where note = '10 stddev') i
	on (a.ip = i.ip and a.event_date = i.event_date)
	left outer join (select cookie, CONCAT('T2 cookie - ', note) note from banned_cookie_butterfly where note = '10 stddev') c
	on (a.cookie = c.cookie)
group by coalesce(p.domain_id,0),
        a.event_date, 
	case when COALESCE(ci.note, c.note, i.note) is not NULL then 1 else 0 end
;
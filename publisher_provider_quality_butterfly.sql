use warehouse_stg;
select 'Overall filter rate:' ayayayayay from publisher_provider_quality_summary_butterfly
limit 1
;

select concat(min(event_date), ' - ', max(event_date)) date_range, round(sum(is_bot*ad_ct)/sum(ad_ct),2) ad_filter_rate,
        round(sum(is_bot*stream_ct)/sum(stream_ct),2) stream_filter_rate
from publisher_provider_quality_summary_butterfly
where event_date >= str_to_date(concat(year(subdate(curdate(), INTERVAL 1 day)), '-', month(subdate(curdate(), INTERVAL 1 day)), '-01'),'%Y-%m-%d')
and event_date < curdate()
;

select '------------' ayayayayay
from publisher_provider_quality_summary_butterfly
limit 1
;

select 'Publisher level filter rate:' ayayayayay 
from publisher_provider_quality_summary_butterfly
limit 1
;

select  d.name domain,
        a.domain_id,
	a.provider,	
        case when a.ad_ct >= 1000 then round(a.ad_ct_bot/a.ad_ct,4) when a.stream_ct >= 1000 then round(a.stream_ct_bot/a.stream_ct,4) end bot_filter,
	round(a.stream_ct_bot/a.stream_ct,4) stream_filter_rate,
        round(a.cookie_ct_neat/a.ip_ct_neat,2) cookie_per_ip,
        round(b.overlap_ip_ct/b.ip_ct,4) cross_visit,
        round(a.ad_ct_neat/a.ip_ct_neat,2) ads_per_ip,
        round(a.stream_ct_neat/a.ip_ct_neat,2) stream_per_ip,
        #sum(a.ip_ct_neat) ip_ct_neat,
        #sum(a.cookie_ct_neat) cookie_ct_neat,
        #sum(a.stream_ct_neat) stream_ct_neat,
        #sum(a.ad_ct_neat) ad_ct_neat,
        #sum(a.player_ct_neat) player_ct_neat,
        #player stats
        #sum(cookie_ct) cookie_ct,
        #sum(a.ip_ct) ip_ct,
        a.player_ct player_ct,
        #sum(a.impression) impression,
        round(a.clicked_player_ct/a.player_ct,4) player_clicked,
        round(a.player_adjusted_volume_ct/a.player_ct,4) player_adjusted_volume,
        round(a.player_muted_ct/a.player_ct,4) player_muted,
        round(a.player_adNotEnabled_ct/a.player_ct,4) player_adNotEnabled,
        round(a.player_adAbsent_ct/a.player_ct,4) player_adAbsent,
        round(a.player_in_iframe_ct/a.player_ct,4) player_in_iframe,
        round(a.player_visible_ct/a.player_ct,4) player_visible,
        #stream stats
        a.stream_ct stream_ct,
        round(a.stream_0_volume_ct/a.stream_ct,4) stream_0_volume,
        round(a.stream_autostart_ct/a.stream_ct,4) stream_autostart,
        #ads stats
        a.ad_ct ad_ct,
        round(a.adClicked2Site/a.ad_ct,4) adClicked2Site,
        a.adSkipped adSkipped,
        a.adsStopped adsStopped,
        a.adError adError
from (
	select a.domain_id,
	c.name provider,
        sum(ip_ct_neat) ip_ct_neat,
        sum(cookie_ct_neat) cookie_ct_neat,
        sum(stream_ct_neat) stream_ct_neat,
        sum(ad_ct_neat) ad_ct_neat,
        sum(player_ct_neat) player_ct_neat,
        #player stats
        sum(cookie_ct) cookie_ct,
        sum(ip_ct) ip_ct,
        sum(player_ct) player_ct,
        sum(impression) impression,
        sum(clicked_player_ct) clicked_player_ct,
        sum(player_adjusted_volume_ct) player_adjusted_volume_ct,
        sum(player_muted_ct) player_muted_ct,
        sum(player_adNotEnabled_ct) player_adNotEnabled_ct,
        sum(case when player_adAbsent_ct > player_ct then player_ct else player_adAbsent_ct end) player_adAbsent_ct,
        sum(player_in_iframe_ct) player_in_iframe_ct,
        sum(player_visible_ct) player_visible_ct,
        #stream stats
        sum(stream_ct) stream_ct,
        sum(IF(is_bot = 1,stream_ct,0)) stream_ct_bot,
        sum(stream_0_volume_ct) stream_0_volume_ct,
        sum(stream_autostart_ct) stream_autostart_ct,
        #ads stats
        sum(ad_ct) ad_ct,
        sum(IF(is_bot = 1, ad_ct, 0)) ad_ct_bot,
        sum(adClicked2Site) adClicked2Site,
        sum(adSkipped) adSkipped,
        sum(adsStopped) adsStopped,
        sum(adError) adError
        from publisher_provider_quality_summary_butterfly a left join client_portal.companies c on (a.company_id = c.id)
        where event_date >= str_to_date(concat(year(subdate(curdate(), INTERVAL 1 day)), '-', month(subdate(curdate(), INTERVAL 1 day)), '-01'),'%Y-%m-%d')
        and event_date < curdate()
	group by domain_id, c.name
        ) a
        left outer join
        (select domain_id,
        sum(ip_ct) ip_ct,
        sum(overlap_ip_ct) overlap_ip_ct
        from overlap_ip_summary_butterfly
        where event_date >= str_to_date(concat(year(subdate(curdate(), INTERVAL 1 day)), '-', month(subdate(curdate(), INTERVAL 1 day)), '-01'),'%Y-%m-%d')
        and event_date < curdate()
	group by domain_id
        ) b
        on (a.domain_id = b.domain_id)
	left outer join client_portal.domains d
        on (a.domain_id = d.id)
;

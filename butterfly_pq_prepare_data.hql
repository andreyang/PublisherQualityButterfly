use ayang;

drop table if exists ay_temp_butterfly_pq;
create table ay_temp_butterfly_pq as
select 	params['_event'] event_type,
		params['client.user_id'] cookie,
                params['client.page_id'] player_instance,
                --replace player_id with domain_id and drop join to players table in the summary step.
                params['client.player_id'] player_id,
		params['detail.media.video_id'] video_id,
		params['detail.player.auto_play'] auto_play,
		params['detail.media.volume'] media_volume,		
		ip,
		time_local,
		concat('${year_yesterday}', '-', '${month_yesterday}', '-', '${date_yesterday}') event_date
from butterfly.data_primitive
where ((y = '${year_yesterday}' and m = '${month_yesterday}' and d= '${date_yesterday}' and h >= '${time_shift}') or (y = '${year_today}' and m = '${month_today}' and d= '${date_today}' and h < '${time_shift}'))
and upper(params['_event']) IN ('AD_PLAY', 'WIDGET_CREATED', 'VIDEO_CONTENT_BEGIN', 'INFO', 'AD_CLICK', 'MUTED', 'VOLUME_CHANGE', 'SLOT_ABSENT', 'SLOT_STOPPED')
and (params['detail.media.video_id'] not in ('889e6b80-0621-012e-2ba9-12313b079c51','68664b27-3510-48f4-a1be-d0d0b64d3115') or params['detail.media.video_id'] is NULL)
;
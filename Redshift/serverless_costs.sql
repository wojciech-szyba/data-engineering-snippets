SELECT DATE_TRUNC('day', start_time),sum(charged_seconds) total_seconds_charged, round(sum(charged_seconds)/60/60) total_hours_charged
FROM SYS_SERVERLESS_USAGE
GROUP BY DATE_TRUNC('day', start_time)
ORDER BY DATE_TRUNC('day', start_time) desc;

SELECT DATE_TRUNC('hour', start_time),sum(charged_seconds) total_seconds_charged, round(sum(charged_seconds)/60/60) total_hours_charged
FROM SYS_SERVERLESS_USAGE
group by DATE_TRUNC('hour', start_time)
order by DATE_TRUNC('hour', start_time) desc;

SELECT DATE_TRUNC('day', record_time),remote_host,database_name,user_name,sum(duration) total_duration_microseconds,count(1) number
FROM SYS_CONNECTION_LOG 
GROUP BY DATE_TRUNC('day', record_time),remote_host,database_name,user_name
ORDER BY DATE_TRUNC('day', record_time) desc, user_name;


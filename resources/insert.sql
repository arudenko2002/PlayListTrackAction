WITH  getToday AS (
  --SELECT CURRENT_DATE()
  SELECT DATE("{ExecutionDate}") as today
),
get_user_products_trackers  AS(
  SELECT
    a.*,
    b.*,
    c.playlist_uri,
    c.report_date,
    c.position,
    c.added_at,
    c.added_by,
    c.track_uri,
    c.track_name,
    --isrc AAA,
    c.artist_uri,
    c.artist_name,
    c.album_type,
    c.album_uri,
    c.album_name
  FROM
    (
    SELECT * FROM swift_trends_alerts.temp_output_table
    where
    -- conopus_id=10057501 or
    conopus_id=10064799
    or conopus_id=10238158
    or conopus_id=10072176
    or conopus_id=10001057
    or conopus_id=10042171
    ) a
  JOIN
    metadata.product b
    ON a.conopus_id=b.master_artist_id
  JOIN
    (
    SELECT
    playlist_uri,
    report_date,
    --c.position,
    added_by,
    track_uri,
    track_name,
    isrc,
    artist_uri,
    artist_name,
    album_type,
    album_uri,
    album_name,
    max(position) as position,
    max(added_at) as added_at
    FROM
    metadata.spotify_playlist_tracks
    WHERE
    LENGTH(isrc)>0
    --AND _PARTITIONTIME>="2017-08-01 00:00:00"
    AND _PARTITIONTIME>=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY))
    --AND _PARTITIONTIME<"2017-08-03 00:00:00"
    AND _PARTITIONTIME<TIMESTAMP((SELECT today FROM getToday))
    --AND playlist_uri="spotify:user:22z5y2tny4iq43jtouhrh76pi:playlist:0mZdLcVBcH6vax0TCmYQXL"
    GROUP BY 1,2,3,4,5,6,7,8,9, 10,11
    ) c ON b.isrc=c.isrc
),

get_details AS (
  SELECT playlist_uri,playlist_id,description,owner_id,owner_uri,type,report_date,name,
  max(followers) as followers,count(playlist_uri)
  FROM metadata.spotify_playlist_details
  WHERE
  --_PARTITIONTIME>='2017-08-01 00:00:00'
  _PARTITIONTIME>=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY))
  --AND _PARTITIONTIME<'2017-08-03 00:00:00'
  AND _PARTITIONTIME<TIMESTAMP((SELECT today FROM getToday))
  GROUP BY 1,2,3,4,5,6,7,8
),


getSet AS (
SELECT * --name, value, SUM(value) OVER (ORDER BY value) AS RunningTotal
FROM
  (SELECT "aaa" AS playlist_uri, "111" as family, "1000" as field, "2017-08-01" as report_date
   UNION ALL
   --SELECT "aaa" AS name, "111" as family, "1000" as field,"2017-08-02" as date
   --UNION ALL
   SELECT "bbb" AS playlist_uri, "222" as family, "2000" as field,"2017-08-01" as report_date
   UNION ALL
   SELECT "bbb" AS playlist_uri, "222" as family, "2000" as field,"2017-08-02" as report_date
   UNION ALL
   --SELECT "ccc" AS name, "333" as family,"2017-08-01" as date
   --UNION ALL
   SELECT "ccc" AS playlist_uri, "333" as family, "2000" as field, "2017-08-02" as report_date
   UNION ALL
   SELECT "ddd" AS playlist_uri, "444" as family, "3000" as field, "2017-08-01" as report_date
   UNION ALL
   SELECT "ddd" AS playlist_uri, "444" as family, "3000" as field, "2017-08-02" as report_date
   )
   ORDER BY playlist_uri,family,field
),

getJOIN AS (
--select * from get_user_products_trackers_details  -- where report_date="2017-08-01";
SELECT d.*,c.action_type FROM (
SELECT
CASE WHEN (a.playlist_uri is not null) THEN a.playlist_uri ELSE b.playlist_uri END as playlist_uri,
CASE WHEN (a.isrc is not null) THEN a.isrc ELSE b.isrc END as isrc,
CASE WHEN (a.report_date is not null) THEN a.report_date ELSE b.report_date END as report_date,
CASE WHEN (a.email is not null) THEN a.email ELSE b.email END as email,
CASE WHEN (a.track_uri is not null) THEN a.track_uri ELSE b.track_uri END as track_uri,
CASE WHEN (
          a.report_date is not null and b.report_date is null
       ) THEN "DROP"
     WHEN (
          a.report_date is null and b.report_date is not null
       ) THEN "ADD"
     WHEN  (
         a.report_date is not null
         OR
         b.report_date is not null
       ) THEN "NOACTION"
END as action_type

FROM (
SELECT email,playlist_uri,isrc,report_date,track_uri
from
--get_user_products_trackers where report_date="2017-08-01"
get_user_products_trackers
where
report_date=DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY)
) a
full join
(
SELECT email,playlist_uri,isrc,report_date,track_uri
from
--get_user_products_trackers where report_date="2017-08-02"
get_user_products_trackers
where report_date=DATE_SUB((SELECT today FROM getToday), INTERVAL 1 DAY)
) b
on a.playlist_uri=b.playlist_uri and a.isrc=b.isrc and a.email=b.email
) c
join
get_user_products_trackers d
on c.playlist_uri=d.playlist_uri and c.isrc=d.isrc and c.report_date=d.report_date and c.email=d.email and c.track_uri=d.track_uri
where action_type <>"NOACTION"
),
get_top_playlists AS (
    SELECT playlist_uri,sum(streams_total) as streams,sum(streams_position_predicted) as estimated_streams
    FROM `umg-data-science.projections.playlist_track_projections`
    WHERE
    _PARTITIONTIME >=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 28 DAY))
    and
    _PARTITIONTIME < TIMESTAMP((SELECT today FROM getToday))
    GROUP BY 1
    ORDER BY streams DESC
    LIMIT 2000
),
get_user_products_trackers_details AS (
  SELECT c.*,
    d.playlist_id,
    d.name,
    d.description,
    d.owner_id,
    d.owner_uri,
    d.followers
    ,case c.action_type
			when 'DROP' then c.report_date
			when 'ADD' then coalesce(DATE(added_at),c.report_date)
	end as action_date
	,case c.action_type
			when 'DROP' then TIMESTAMP(c.report_date)
			when 'ADD' then coalesce(added_at,TIMESTAMP(c.report_date))
	end as action_timestamp,
	e.streams,
	e.estimated_streams,
	CASE WHEN f.country IS NULL THEN "Unknown" ELSE f.country END as country
	--"USA" as country
  --FROM get_user_products_trackers c
  FROM getJOIN c
  JOIN get_details d
  ON c.playlist_uri=d.playlist_uri
  AND c.report_date=d.report_date
  JOIN get_top_playlists e
  ON c.playlist_uri=e.playlist_uri
  LEFT JOIN metadata.playlist_geography f
  ON c.playlist_uri=f.playlist_uri
)
select * from get_user_products_trackers_details c
--where product_title="'Till I Collapse"
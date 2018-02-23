SELECT * FROM
  metadata.temp_output_table  a
JOIN
  metadata.product b ON a.conopus_id=b.master_artist_id
OUTER JOIN
  metadata.spotify_playlist_tracks c ON b.isrc=c.isrc OR b.product_title=c.track_name
JOIN (
  SELECT playlist_uri,playlist_id,name,description,owner_id,owner_uri,type,
  max(report_date) as report_date,max(followers) as followers,count(playlist_uri) from metadata.spotify_playlist_details
  GROUP BY 1,2,3,4,5,6,7
  ) d ON c.playlist_uri=d.playlist_uri AND DATE(_PARTITIONTIME)=report_date
WHERE
  LENGTH(b.isrc)>0
  --AND b.product_title!=c.track_name
  AND c._PARTITIONTIME>='2017-07-31'
  AND c._PARTITIONTIME<'2017-08-02'
LIMIT 1000;
----------------------------------------------------------------
SELECT * FROM metadata.product
WHERE conopus_id='10042171';

SELECT * FROM metadata.product
WHERE master_artist_id in (10042171,10238158,10001057,10064799,10057501,10072176,10060934,10040570,10022047,10042171);

rez=18608
------------------------------------------------------------------
SELECT * FROM
  metadata.temp_output_table  a
JOIN
  metadata.product b ON a.conopus_id=b.master_artist_id

rez=23835
------------------------------------------------------------------
SELECT
lastname,firstname,conopus_id,b.isrc,product_title, c.track_name,c.playlist_uri,c.report_date,c.position,c.track_uri,c.artist_name
FROM
  metadata.temp_output_table  a
JOIN
  metadata.product b ON a.conopus_id=b.master_artist_id
JOIN
  metadata.spotify_playlist_tracks c
  ON
  b.isrc=c.isrc
  --AND
  --b.product_title=c.track_name

WHERE
  LENGTH(b.isrc)>0
  AND b.product_title!=c.track_name
  AND (c._PARTITIONTIME>="2017-07-31 00:00:00" OR c._PARTITIONTIME<"2017-08-02 00:00:00")
  LIMIT 20;
  -----------------------------------------------------------------

  SELECT COUNT(*) FROM (
  SELECT UNIQUE(playlist_uri) from metadata.spotify_playlist_tracks
  WHERE isrc="USUM71204501"
  )

  SELECT playlist_uri from metadata.spotify_playlist_tracks
  WHERE isrc="USUM71204501"
  AND _PARTITIONTIME>="2017-07-31 00:00:00"
  AND _PARTITIONTIME<"2017-08-01 00:00:00"
  ;

SELECT UNIQUE(playlist_uri) from metadata.spotify_playlist_tracks
WHERE isrc="USUM71204501"
AND _PARTITIONTIME>="2017-07-31 00:00:00"
AND _PARTITIONTIME<"2017-08-01 00:00:00"
;

SELECT playlist_uri from metadata.spotify_playlist_tracks
WHERE isrc="USUM71204501"
AND _PARTITIONTIME>="2017-07-31 00:00:00"
AND _PARTITIONTIME<"2017-08-01 00:00:00"
;

-------------------------------------------------------------------
SELECT * from metadata.spotify_playlist_details
WHERE playlist_uri="spotify:user:spotify:playlist:5sTJVVDhI6Y0flMI2habTQ"
LIMIT 100
--SELECT count(UNIQUE(playlist_uri)) from metadata.spotify_playlist_details
;
----------------------------------------------------------------------

SELECT playlist_uri,playlist_id,name,description,owner_id,owner_uri,type,
max(report_date),max(followers),count(playlist_uri) from metadata.spotify_playlist_details

--WHERE playlist_uri="spotify:user:spotify:playlist:5sTJVVDhI6Y0flMI2habTQ"
GROUP BY 1,2,3,4,5,6,7
ORDER BY playlist_uri
LIMIT 100
--SELECT count(UNIQUE(playlist_uri)) from metadata.spotify_playlist_details
;
-----------------------------------------------------------------------

SELECT count(*) FROM
  metadata.temp_output_table  a
JOIN
  metadata.product b ON a.conopus_id=b.master_artist_id
JOIN (
SELECT
*
FROM
  metadata.spotify_playlist_tracks
  WHERE
  LENGTH(isrc)>0
  AND (_PARTITIONTIME>="2017-08-01 00:00:00" AND _PARTITIONTIME<"2017-08-03 00:00:00")
  ) c ON b.isrc=c.isrc -- AND b.product_title=c.track_name
JOIN (
  SELECT playlist_uri,playlist_id,name,description,owner_id,owner_uri,type,
  max(report_date) as report_date,max(followers) as followers,count(playlist_uri) from metadata.spotify_playlist_details
  WHERE
  --LENGTH(isrc)>0
  --AND b.product_title!=c.track_name
  _PARTITIONTIME>='2017-08-01 00:00:00'
  AND _PARTITIONTIME<'2017-08-03 00:00:00'
  GROUP BY 1,2,3,4,5,6,7
  ) d ON c.playlist_uri=d.playlist_uri -- AND DATE(c._PARTITIONTIME)=d.report_date
---------------------------------------------------------------------------
SELECT
--playlist_uri,
--report_date,
--position,
--added_at,
--added_by,
--track_uri,
--track_name,
--isrc,
--artist_uri,
--artist_name,
--album_type,
--album_uri,
--album_name
count(*)
FROM
  metadata.spotify_playlist_tracks
  WHERE
  LENGTH(isrc)>0
  AND (_PARTITIONTIME>="2017-08-01 00:00:00" AND _PARTITIONTIME<"2017-08-02 00:00:00")
  LIMIT 100;
  -----------------------------------------------------------------------------------

  SELECT
  lastName,
  firstName,
  email,
  conopus_id,
  product_id,
  product_title,
  upc,
  isrc,
  master_artist_id,
  master_artist,
  master_track_id,
  master_track,
  master_album_id,
  master_album,
  resource_title,
  release_title,
  project_id,
  project_title,
  sap_hfm_rep_owner,
  sap_segment,
  sap_profit_center,
  sap_financial_label,
  r2_company,
  r2_division,
  r2_label,
  sap_sales_rep_owner,
  sap_source_rep_owner,
  sap_domestic_flag,
  product_release_date,
  first_release_activity_week,
  earliest_resource_release_date,
  original_release_date,
  r2_family_id,
  r2_Family_name,
  configuration_code,
  configuration_description,
  resource_version_title,
  release_version_title,
  genre_code,
  genre_name,
  sub_genre_code,
  sub_genre_name,
  product_group,
  product_life_cycle_type,
  r2_project_number,
  r2_project_name,
  project_release_date,
  first_project_activity_week,
  load_datetime
  FROM (
  SELECT
  lastName,
  firstName,
  email,
  conopus_id,
  product_id,
  product_title,
  upc,
  isrc,
  master_artist_id,
  master_artist,
  master_track_id,
  master_track,
  master_album_id,
  master_album,
  resource_title,
  release_title,
  project_id,
  project_title,
  sap_hfm_rep_owner,
  sap_segment,
  sap_profit_center,
  sap_financial_label,
  r2_company,
  r2_division,
  r2_label,
  sap_sales_rep_owner,
  sap_source_rep_owner,
  sap_domestic_flag,
  product_release_date,
  first_release_activity_week,
  earliest_resource_release_date,
  original_release_date,
  r2_family_id,
  r2_Family_name,
  configuration_code,
  configuration_description,
  resource_version_title,
  release_version_title,
  genre_code,
  genre_name,
  sub_genre_code,
  sub_genre_name,
  product_group,
  product_life_cycle_type,
  r2_project_number,
  r2_project_name,
  project_release_date,
  first_project_activity_week,
  load_datetime
  FROM
    metadata.temp_output_table  a
  JOIN
    metadata.product b ON a.conopus_id=b.master_artist_id
    where a.conopus_id=10057501
  )

---------------------------------------------------------------

SELECT
lastName,
firstName,
email,
conopus_id,

product_id,
product_title,
upc,
b.isrc,
master_artist_id,
master_artist,
master_track_id,
master_track,
master_album_id,
master_album,
resource_title,
release_title,
project_id,
project_title,
sap_hfm_rep_owner,
sap_segment,
sap_profit_center,
sap_financial_label,
r2_company,
r2_division,
r2_label,
sap_sales_rep_owner,
sap_source_rep_owner,
sap_domestic_flag,
product_release_date,
first_release_activity_week,
earliest_resource_release_date,
original_release_date,
r2_family_id,
r2_Family_name,
configuration_code,
configuration_description,
resource_version_title,
release_version_title,
genre_code,
genre_name,
sub_genre_code,
sub_genre_name,
product_group,
product_life_cycle_type,
r2_project_number,
r2_project_name,
project_release_date,
first_project_activity_week,
load_datetime



FROM (
SELECT
lastName,
firstName,
email,
conopus_id,

product_id,
product_title,
upc,
b.isrc,
master_artist_id,
master_artist,
master_track_id,
master_track,
master_album_id,
master_album,
resource_title,
release_title,
project_id,
project_title,
sap_hfm_rep_owner,
sap_segment,
sap_profit_center,
sap_financial_label,
r2_company,
r2_division,
r2_label,
sap_sales_rep_owner,
sap_source_rep_owner,
sap_domestic_flag,
product_release_date,
first_release_activity_week,
earliest_resource_release_date,
original_release_date,
r2_family_id,
r2_Family_name,
configuration_code,
configuration_description,
resource_version_title,
release_version_title,
genre_code,
genre_name,
sub_genre_code,
sub_genre_name,
product_group,
product_life_cycle_type,
r2_project_number,
r2_project_name,
project_release_date,
first_project_activity_week,
load_datetime,

--playlist_uri,
--report_date,
--position,
--added_at,
--added_by,
--track_uri,
--track_name,
--artist_uri,
--artist_name,
--album_type,
--album_uri,
--album_name


FROM
(
SELECT * FROM
  metadata.temp_output_table
  where conopus_id=10057501
  ) a
JOIN
metadata.product b
ON a.conopus_id=b.master_artist_id
JOIN
(
SELECT
*
--playlist_uri,
--report_date,
--position,
--added_at,
--added_by,
--track_uri,
--track_name,
--artist_uri,
--artist_name,
--album_type,
--album_uri,
--album_name
FROM metadata.spotify_playlist_tracks
WHERE
LENGTH(isrc)>0
AND _PARTITIONTIME>="2017-08-01 00:00:00"
AND _PARTITIONTIME<"2017-08-02 00:00:00"
) c
ON b.isrc=c.isrc
)

----------------------------------------------------------------------------------------------------
WITH aaa AS
(
SELECT a.*,b.*,
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
c.album_name --,
----c.track_name,c.playlist_uri,c.report_date,count(*)
,
----d.report_date
----d.playlist_uri
d.playlist_id,
d.name,
d.description,
d.owner_id,
d.owner_uri,
d.followers
--d.playlist_image_url
FROM
(
SELECT * FROM metadata.temp_output_table
  where conopus_id=10057501
  ) a
JOIN
metadata.product b
ON a.conopus_id=b.master_artist_id
JOIN (
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
AND _PARTITIONTIME>="2017-08-01 00:00:00"
AND _PARTITIONTIME<"2017-08-03 00:00:00"
GROUP BY 1,2,3,4,5,6,7,8,9, 10,11
) c ON b.isrc=c.isrc
JOIN (
SELECT playlist_uri,playlist_id,description,owner_id,owner_uri,type,report_date,name,
  max(followers) as followers,count(playlist_uri)
  FROM metadata.spotify_playlist_details
  WHERE
  _PARTITIONTIME>='2017-08-01 00:00:00'
  AND _PARTITIONTIME<'2017-08-03 00:00:00'
  GROUP BY 1,2,3,4,5,6,7,8
) d
ON c.playlist_uri=d.playlist_uri AND c.report_date=d.report_date
--WHERE c.playlist_uri="spotify:user:22n4yo5wsbi4oobdgvzrr2pxq:playlist:0reK31pofslH4FFFMLrM3i"
--GROUP BY c.track_name,c.playlist_uri,c.report_date
--HAVING count(*)>1
--ORDER BY c.track_name
--LIMIT 100
)
,bbb AS
(
SELECT a.*,b.*,
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
 --c.track_name,c.playlist_uri,c.report_date,count(*)
FROM
(
SELECT * FROM metadata.temp_output_table
  where conopus_id=10057501
  ) a
JOIN
metadata.product b
ON a.conopus_id=b.master_artist_id
JOIN (
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
AND _PARTITIONTIME>="2017-08-02 00:00:00"
AND _PARTITIONTIME<"2017-08-04 00:00:00"
GROUP BY 1,2,3,4,5,6,7,8,9, 10,11
) c ON b.isrc=c.isrc
),
error as (
SELECT * FROM -- bbb
metadata.spotify_playlist_tracks
WHERE playlist_uri="spotify:user:nathaniel.gudiel619:playlist:1T6IRRGT73N6qRfMTUh82J"
AND report_date="2017-08-03"
ORDER BY position
LIMIT 1000
),
distonct as (
SELECT
c.playlist_uri,
c.report_date,
--c.position,
c.added_by,
c.track_uri,
c.track_name,
c.isrc,
c.artist_uri,
c.artist_name,
c.album_type,
c.album_uri,
c.album_name,
max(c.position) as position,
max(added_at) as added_at
FROM
metadata.spotify_playlist_tracks c
WHERE playlist_uri="spotify:user:22n4yo5wsbi4oobdgvzrr2pxq:playlist:0reK31pofslH4FFFMLrM3i"
AND report_date="2017-08-02"
AND (track_name="Collard Greens" OR track_name="Red Cup (feat. T-Pain, Kid Ink & B.o.B.)")
GROUP BY 1,2,3,4,5,6,7,8,9, 10,11
--ORDER BY position
LIMIT 1000)

select * from aaa-- where report_date="2017-08-01";

--SELECT * FROM metadata.spotify_playlist_tracks
--WHERE playlist_uri="spotify:user:nathaniel.gudiel619:playlist:1T6IRRGT73N6qRfMTUh82J" and report_date="2017-08-02"
--ORDER BY track_name,position
----------------------------------------------------------------------

WITH  getToday AS (
  --SELECT CURRENT_DATE()
  SELECT DATE("2017-08-03") as today
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
    SELECT * FROM metadata.temp_output_table where conopus_id=10057501
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
SELECT d.*,c.action FROM (
SELECT
CASE WHEN (a.playlist_uri is not null) THEN a.playlist_uri ELSE b.playlist_uri END as playlist_uri,
CASE WHEN (a.isrc is not null) THEN a.isrc ELSE b.isrc END as isrc,
CASE WHEN (a.report_date is not null) THEN a.report_date ELSE b.report_date END as report_date,
CASE WHEN (
          a.report_date is not null and b.report_date is null
       ) THEN "DELETED"
     WHEN (
          a.report_date is null and b.report_date is not null
       ) THEN "ADDED"
     WHEN  (
         a.report_date is not null
         OR
         b.report_date is not null
       ) THEN "NOACTION"
END as action
FROM (
SELECT playlist_uri,isrc,report_date
from
--get_user_products_trackers where report_date="2017-08-01"
get_user_products_trackers
where
report_date=DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY)
) a
full join
(
SELECT playlist_uri,isrc,report_date
from
--get_user_products_trackers where report_date="2017-08-02"
get_user_products_trackers
where report_date=DATE_SUB((SELECT today FROM getToday), INTERVAL 1 DAY)
) b
on a.playlist_uri=b.playlist_uri and a.isrc=b.isrc
) c
join
get_user_products_trackers d
on c.playlist_uri=d.playlist_uri and c.isrc=d.isrc and c.report_date=d.report_date
where action <>"NOACTION"
),
get_user_products_trackers_details AS (
  SELECT c.*,
    d.playlist_id,
    d.name,
    d.description,
    d.owner_id,
    d.owner_uri,
    d.followers
  --FROM get_user_products_trackers c
  FROM getJOIN c
  JOIN get_details d
  ON c.playlist_uri=d.playlist_uri
  AND c.report_date=d.report_date
)
select * from get_user_products_trackers_details c;

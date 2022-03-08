create schema assignment;

use assignment;

create table station
(
id int, 
station_name text, 
lat	double,
lon double,	
dock_count	int,
city text,
installation_date text, -- STR_TO_DATE(installation_date, '%m/%d/%Y'
primary key (id)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/station.csv' 
INTO TABLE station 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

create table status
(
station_id	int,
bikes_available	int,
docks_available	int,
time datetime,
foreign key (station_id) references station(id)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/status.csv' 
INTO TABLE status
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

create table trip
(
unknow_col int,
id	int,
duration double,	
start_date	text,
start_station_name	text,
start_station_id int,
end_date text,
end_station_name text,
end_station_id int,
bike_id	int,
subscription_type text,
primary key(id),
foreign key (start_station_id) references station(id) ON DELETE SET NULL ON UPDATE CASCADE,
foreign key (end_station_id) references station(id) ON DELETE SET NULL ON UPDATE CASCADE
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/trip.csv' 
INTO TABLE trip
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

create table weather
(
unknow_col int,
id int,	
weather_date text,
Temperature	varchar(16),
Humidity varchar(16),
Dew_Point varchar(16),	
mean_wind_speed_mph	varchar(16),
Pincode int,
primary key(id)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/weather.csv' 
INTO TABLE weather
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select count(distinct id) from station;
select count(distinct bike_id) from trip;
select count(distinct id) from trip;


select bike_id, count(start_station_id) as station_cnt from trip
group by bike_id
order by station_cnt;

select start_station_id, count(bike_id) as bike_cnt from trip
group by start_station_id
order by bike_cnt;


select pincode, count(id) as cnt from weather
group by pincode
order by cnt;

select city, count(id) as cnt from station
group by city
order by cnt;

with cte as
(select *, 
case when city = 'Palo Alto' then 94301
     when city = 'San Francisco' then 94107
     when city = 'Redwood City' then 94063
     when city = 'Mountain View' then 94041
     when city = 'San Jose' then 95113
end as Pincode
from station)
select lon, count(distinct w.pincode) as cnt from weather w join cte ct on w.pincode=ct.pincode
group by lon
order by cnt;

with cte as
(select *, 
case when city = 'Palo Alto' then 94301
     when city = 'San Francisco' then 94107
     when city = 'Redwood City' then 94063
     when city = 'Mountain View' then 94041
     when city = 'San Jose' then 95113
end as Pincode
from station)
select lat, count(distinct w.pincode) as cnt from weather w join cte ct on w.pincode=ct.pincode
group by lat
order by cnt;


with cte as
(select *, 
case when city = 'Palo Alto' then 94301
     when city = 'San Francisco' then 94107
     when city = 'Redwood City' then 94063
     when city = 'Mountain View' then 94041
     when city = 'San Jose' then 95113
end as Pincode
from station)
select w.pincode, count(distinct lon) as cnt from weather w join cte ct on w.pincode=ct.pincode
group by w.pincode
order by cnt;

with cte as
(select *, 
case when city = 'Palo Alto' then 94301
     when city = 'San Francisco' then 94107
     when city = 'Redwood City' then 94063
     when city = 'Mountain View' then 94041
     when city = 'San Jose' then 95113
end as Pincode
from station)
select w.pincode, count(distinct lat) as cnt from weather w join cte ct on w.pincode=ct.pincode
group by w.pincode
order by cnt;

select weather_date, count(mean_wind_speed_mph) as cnt from weather
where weather_date = '8/29/2013'
group by weather_date;

select * from trip where id in
(select max(id) from trip
union
select min(id) from trip );


select * from trip where id in
(select max(id) from trip
union
select min(id) from trip )
and id is not null;

select max( STR_TO_DATE(start_date, '%m/%d/%Y')) from trip
union
select min( STR_TO_DATE(start_date, '%m/%d/%Y')) from trip;

select avg(duration) from trip;

select avg(duration) from trip
where start_station_id = end_station_id;

select a.bike_id, a.sd from
(select bike_id, sum(duration) sd, dense_rank() over(order by sum(duration) desc) as rnk from trip
group by bike_id)a
where a.rnk = 1;



select start_station_name from
(select start_station_name, dense_rank() over(order by count(start_station_name)) as rnk from trip
group by start_station_name)a
where a.rnk < 11;

select * from station s join status t on s.id = t.station_id
where t.time like '%2013-08-29%'
and s.id = 2
and bikes_available>3;

select id, station_name, next_station_id, next_station_name,
acos(
cos(radians( st.lat ))
* cos(radians( st.lead_lat ))
* cos(radians( st.lon ) - radians( st.lead_long ))
+ sin(radians( st.lat ))
* sin(radians( st.lead_lat ))
) AS consecutiveStationDistance from (select *,
LEAD(station.id) OVER(ORDER BY station.id) as next_station_id,
LEAD(station.station_name) OVER(ORDER BY station.id) as next_station_name,
LEAD(station.lat) OVER(ORDER BY station.id) as lead_lat,
LEAD(station.lon) OVER(ORDER BY station.id ) as lead_long
from station) AS st
where st.next_station_id is not null;

select l.unpopular_station_name from
(select m.*, LAG(m.unpopular_station_id, 1, 0) OVER(ORDER BY m.rnkcomb) as prev_unpop_station_id from
(select k.*, dense_rank() over( order by (k.distrank+k.cntrnk)) as rnkcomb from
(select d.*, 
dense_rank() over(order by d.consecutiveStationDistance) as distrank,
dense_rank() over(order by d.unpopular_station_cnt) as cntrnk
from
(select  a.*, t1.cnt as idcnt, t2.cnt as nextidcnt,
case when t1.cnt > t2.cnt then a.next_station_name 
     else a.station_name
end as unpopular_station_name,
case when t1.cnt > t2.cnt then t2.start_station_id 
     else t1.start_station_id
end as unpopular_station_id,
case when t1.cnt > t2.cnt then t2.cnt 
     else t1.cnt
end as unpopular_station_cnt
from
(select id, station_name, next_station_id, next_station_name,
acos(
cos(radians( st.lat ))
* cos(radians( st.lead_lat ))
* cos(radians( st.lon ) - radians( st.lead_long ))
+ sin(radians( st.lat ))
* sin(radians( st.lead_lat ))
) AS consecutiveStationDistance from (select *,
LEAD(station.id) OVER(ORDER BY station.id) as next_station_id,
LEAD(station.station_name) OVER(ORDER BY station.id) as next_station_name,
LEAD(station.lat) OVER(ORDER BY station.id) as lead_lat,
LEAD(station.lon) OVER(ORDER BY station.id ) as lead_long
from station) AS st)a join (select start_station_id, count(id) as cnt from trip
group by start_station_id) t1 on a.id = t1.start_station_id
join (select start_station_id, count(id) as cnt from trip
group by start_station_id) t2 on a.next_station_id = t2.start_station_id
where a.next_station_id is not null)d)k
order by (k.distrank + k.cntrnk))m
where m.rnkcomb<5)l
where l.prev_unpop_station_id != unpopular_station_id-1;



select avg(bikes_available), avg(docks_available) from status
where station_id = 2;


select start_station_name, count(start_station_name) from trip
group by start_station_name
order by  count(start_station_name) desc;
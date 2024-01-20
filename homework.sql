select date(lpep_pickup_datetime), max(trip_distance) from green_taxi_data group by date(lpep_pickup_datetime) order by max(trip_distance) desc;

select * from green_taxi_data;

select * from zones;

select z."Borough", sum(td."total_amount") from zones z, green_taxi_data td where td."PULocationID" = z."LocationID" and date(td."lpep_pickup_datetime") = '2019-09-18' group by z."Borough"

select zdo."Zone", td."tip_amount" from green_taxi_data td JOIN zones zpu ON zpu."LocationID" = td."PULocationID" JOIN zones zdo ON zdo."LocationID" = td."DOLocationID" where zpu."Zone" = 'Astoria' and EXTRACT(MONTH FROM td."lpep_pickup_datetime") = 9 order by (td."tip_amount") desc


Question 1:  Knowing docker tags  ( Multiple choice)
Which tag has the following text? - Write the image ID to the file
*
--iidfile string



Question 2:  Understanding docker first run  (Multiple choice)
How many python packages/modules are installed?
*

3



select count(*)
from green_taxi_data
where date(lpep_pickup_datetime)='2019-01-15'
and date(lpep_dropoff_datetime)  ='2019-01-15';

-- 20530

SELECT lpep_pickup_datetime,lpep_dropoff_datetime
	FROM public.green_taxi_data
where 	trip_distance = (select max(trip_distance) from public.green_taxi_data);
-- 2019-01-15

select count(*), passenger_count
	FROM public.green_taxi_data
where date(lpep_pickup_datetime) = '2019-01-01'
	and date(lpep_pickup_datetime) = '2019-01-01'
	group by passenger_count;

-- 2: 1282 ; 3: 254

select z1."Zone" from green_taxi_data left join zones z1
on z1."LocationID" = green_taxi_data."DOLocationID" left join zones z2
on z2."LocationID" = green_taxi_data."PULocationID"
where    z2."Zone" = 'Astoria'
order by tip_amount desc
limit 1 ;
-- Long Island City/Queens Plaza
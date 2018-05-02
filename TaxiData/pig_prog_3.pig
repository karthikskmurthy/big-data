trip_data = LOAD '/user/root/spark_assignment/input_data/yellow_tripdata_*' USING PigStorage(',') AS  
            (VendorID:int,
             tpep_pickup_datetime:chararray,
	     tpep_dropoff_datetime:chararray,
	     passenger_count:int,
	     trip_distance:double,
	     RatecodeID:int,
	     store_and_fwd_flag:chararray,
	     PULocationID:int,
	     DOLocationID:int,
	     payment_type:int,
	     fare_amount:double,
	     extra:double,
	     mta_tax:double,
 	     tip_amount:double,
	     tolls_amount:double,
	     improvement_surcharge:double,
	     total_amount:double);

groupd = group trip_data by payment_type;

count_groupd = foreach groupd generate group , COUNT(trip_data) as payment_count;

filter_data = filter count_groupd by payment_count>0;

order_data = order filter_data by payment_count asc;
          
STORE order_data INTO '/user/root/pig_assignment/output/group_by';

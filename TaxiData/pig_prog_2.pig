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
filter1 = filter trip_data by RatecodeID == 4;
          
STORE filter1 INTO '/user/root/pig_assignment/output1/filter';

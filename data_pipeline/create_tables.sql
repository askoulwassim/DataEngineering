CREATE TABLE public.trips (
	trip_id int4 IDENTITY(1,1) NOT NULL,
	start_station_id int4 NOT NULL,
	end_station_id int4 NOT NULL,
	start_time timestamp NOT NULL,
	end_time timestamp NOT NULL,
	bike_id int4,
	user_type string,
	birth_year int4,
	gender int4,
	duration_sec int4
	CONSTRAINT trip_pkey PRIMARY KEY (trip_id)
);

CREATE TABLE public.routes (
	route_id int4 NOT NULL,
	start_street_id int4 NOT NULL,
	end_street_id int4 NOT NULL,
	inst_date timestamp NOT NULL,
	mod_date timestamp NOT NULL,
	borough int4,
	facility_cl int4,
	on_off_set boolean,
	all_classes int4,
	bike_direction varchar(4),
	lane_count int4,
	number_nodes int4
	CONSTRAINT route_pkey PRIMARY KEY (route_id)
);

CREATE TABLE public.stations (
	station_id int4 NOT NULL PRIMARY KEY,
	station_name varchar(256),
	station_lattitude numeric(18,0),
	station_longitude numeric(18,0)
);

CREATE TABLE public.streets (
	street_id int4 IDENTITY(1,1) NOT NULL PRIMARY KEY,
	street_name varchar(256),
	street_lattitude numeric(18,0),
	street_longitude numeric(18,0)
);

CREATE TABLE public.staging_citi (
	duration_sec int4,
	start_time timestamp,
	end_time timestamp,
	start_station_id int4,
	start_station_name varchar(256),
	start_station_latitude numeric(18,0),
	start_station_longitude numeric(18,0),
	end_station_id int4,
	end_station_name varchar(256),
	end_station_latitude numeric(18,0),
	end_station_longitude numeric(18,0),
	bike_id int4,
	user_type varchar(256),
	birth_year int4,
	gender int4,
);

CREATE TABLE public.staging_nyc (
	route_name varchar(256),
	borough int4,
	route_id int4,
	facilitycl varchar(32),
	start_street_name varchar(256),
	end_street_name varchar(256),
	on_off_set varchar(16),
	all_classes varchar(32),
	inst_date timstamp,
	mod_date timestamp,
	bike_direction varchar(8),
	lane_count int4,
	number_nodes int4,
	start_street_latitude numeric(18,0),
	start_street_longitude numeric(18,0),
	end_street_latitude numeric(18,0),
	end_street_longitude numeric(18,0)
);

CREATE TABLE public."time_t" (
	"time" timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (time)
);

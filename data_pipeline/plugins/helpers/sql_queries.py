class SqlQueries:
    station_table_insert = ("""
        SELECT distinct start_station_id + end_station_id station_id, start_station_name + end_station_name station_name,\
                        start_station_lattitude + end_station_lattitude station_lattitude, start_station_longitude + end_station_longitude station_longitude
        FROM staging_citi
    """)
    
    street_table_insert = ("""
        SELECT distinct start_street_name + end_street_name street_name,\
                        start_street_lattitude + end_street_lattitude street_lattitude, start_street_longitude + end_street_longitude street_longitude
        FROM staging_nyc
    """)
    
    
    trip_table_insert = ("""
        SELECT distinct start_station_id, end_station_id, start_time, end_time, bike_id, user_type, birth_year, gender, duration_sec
        FROM staging_citi
    """)
        
    route_table_insert = ("""
        SELECT distinct s.route_id, st.street_id start_street_id, st.street_id end_street_id, 
                        s.inst_date, s.mod_date, s.borough, s.facility_cl, s.on_off_set, s.all_classes, s.bike_direction, s.lane_count
        FROM staging_nyc s
        JOIN streets st ON s.start_street_name = st.street_name AND s.end_street_name = st.street_name
    """)
        
    time_table_insert = ("""
        SELECT distinct t.start_time + t.end_time + r.inst_date + r.mod_date, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM trips t
        JOIN routes r ON t.start_time = r.inst_date AND t.end_time = r.mod_date
    """)

    
    
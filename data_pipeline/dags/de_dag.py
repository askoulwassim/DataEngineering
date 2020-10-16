from datetime import datetime, timedelta
import os

import requests, zipfile, io
import json
import pandas as pd

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'wassim_askoul',
    'start_date': datetime(2017, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False,
    'schedule_interval': '@monthly',
    'email_on_retry': False
}

dag = DAG('udacity_dataengineering_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


def load_transform_nyc_data(**kwargs):
    
    r_nycbike = requests.get('https://data.cityofnewyork.us/resource/cc5c-sm6z.json')
    df_nycbike = pd.read_json(r_nycbike.text)
    df_nycbike = pd.DataFrame(df_nycbike)
    
    df_nycbike = df_nycbike.join(pd.json_normalize(df_nycbike.pop('the_geom')))
    df_cleaning = df_nycbike.coordinates.apply(pd.Series)
    df_cleaning.columns = ['iter1']
    
    num_nodes = []
    start_street_latitude = []
    start_street_longitude = []
    end_street_latitude = []
    end_street_longitude = []
    for i in df_cleaning.iter1:
        num_nodes.append(len(i))
        start_street_latitude.append(float(i[0][1]))
        start_street_longitude.append(float(i[0][0]))
        end_street_latitude.append(float(i[len(i)-1][1]))
        end_street_longitude.append(float(i[len(i)-1][0]))

    d = {'num_nodes' : num_nodes, 'start_street_latitude' : start_street_latitude,
    'start_street_longitude' : start_street_longitude, 'end_street_latitude' : end_street_latitude,
    'end_street_longitude' : end_street_longitude}
    df_clean = pd.DataFrame(d)
    
    df_nycbike = df_nycbike.join(df_clean)
    
    df_nycbike = df_nycbike.drop(['tf_facilit', 'ft_facilit', 'comments', 'type', 'coordinates'], axis=1)
    
    df_nycbike.rename(columns = {'street' : 'route_name', 'boro' : 'borough', 'segmentid' : 'route_id', 
                             'facilitycl' : 'facility_cl', 'fromstreet' : 'start_street_name', 'tostreet' : 'end_street_name', 
                             'onoffst' : 'on_off_set', 'allclasses' : 'all_classes',
                             'instdate' : 'inst_date', 'moddate' : 'mod_date', 'bikedir' : 'bike_direction',
                             'lanecount' : 'lane_count', 'num_nodes' : 'number_nodes'}, inplace = True)
    
    df_nycbike.to_csv(r'/home/workspace/data_pipeline/nyc_data.csv', header=True, index=True, sep=',', mode='a')
    
    return('NYC data loaded and transformed!')
                                     
load_transform_nyc = PythonOperator(
    task_id='loading_transforming_nyc_data',
    provide_context=True,
    python_callable=load_transform_nyc_data,
    dag=dag
)

stage_nyc_to_redshift = StageToRedshiftOperator(
    task_id='Stage_nyc',
    dag=dag,
    table="staging_nyc",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_path="/home/workspace/data_pipeline/nyc_data.csv",
    file_type="csv",
)

def unzip_csv(**kwargs):
    
    r_citi = requests.get('https://s3.amazonaws.com/tripdata/(%d)-citibike-tripdata.csv.zip' % (''.join(list(ds)[0:4]+list(ds)[5:7])))
    z_citi = zipfile.ZipFile(io.BytesIO(r_citi.content))
    z_citi.extractall('/home/workspace/data_pipeline/unzipped_data')
    
    return('File (%d) unzipped!' %loc)
                     
unzip_file = PythonOperator(
    task_id='unzipping_csv_file',
    provide_context=True,
    python_callable=unzip_csv,
    dag=dag
)

stage_citi_to_redshift = StageToRedshiftOperator(
    task_id='Stage_citi',
    dag=dag,
    table="staging_citi",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    csv_path="/home/workspace/data_pipeline/unzipped_data/*",
    file_type="csv"
)

load_trips_table = LoadFactOperator(
    task_id='Load_trips_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "trips",
    sql = "trip_table_insert",
    append_mode = False
)

load_routes_table = LoadFactOperator(
    task_id='Load_routes_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "routes",
    sql = "route_table_insert",
    append_mode = False
)

load_station_dimension_table = LoadDimensionOperator(
    task_id='Load_station_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "stations",
    sql = "station_table_insert",
    append_mode = False
)

load_street_dimension_table = LoadDimensionOperator(
    task_id='Load_street_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "streets",
    sql = "street_table_insert",
    append_mode = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "time_t",
    sql = "time_table_insert",
    append_mode = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables = ['trips', 'routes', 'stations', 'trips_routes', 'streets', 'time_t']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks 
#

start_operator >> unzip_file            
start_operator >> load_transform_nyc

unzip_file >> stage_citi_to_redshift                       
load_transform_nyc >> stage_nyc_to_redshift                       
                      
stage_citi_to_redshift >> load_station_dimension_table
stage_nyc_to_redshift >> load_routes_table
                      
load_street_dimension_table << load_routes_table                       

load_trips_table << load_station_dimension_table

load_trips_table >> load_time_dimension_table
load_routes_table >> load_time_dimension_table

run_quality_checks << load_station_dimension_table
run_quality_checks << load_time_dimension_table

run_quality_checks >> end_operator
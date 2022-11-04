import time  
import numpy as np  
import pandas as pd  
import plotly.express as px 
import streamlit as st  
from cassandra.cluster import Cluster

st.set_page_config(
  page_title="Real-Time NYC Taxi Dashboard",
  page_icon="✅",
  layout="wide",
)

session = Cluster().connect()
session.set_keyspace("nyc_taxi")

@st.experimental_memo
def average_trip_distance():
  batch_query = '''
                  SELECT total_trip_distance, total_rides, started_at, ended_at 
                  FROM total_trip_distance_batch 
                  LIMIT 1 ;
                '''
  trip_distance_batch = session.execute(batch_query).all()
  total_trip_distance_batch, total_rides_batch, ended_at, started_at  = trip_distance_batch[0]

  speed_query = f'''
                  SELECT SUM(trip_distance) as total_trip_distance, 
                         COUNT(*) as total_rides 
                  FROM total_trip_distance_speed 
                  WHERE created_at >= '{started_at}' 
                    AND created_at <= '2022-11-03 15:06:10.388000+0000' 
                  ALLOW FILTERING ;
                '''
  trip_distance_speed = session.execute(speed_query).all()
  total_trip_distance_speed, total_rides_speed = trip_distance_speed[0]

  average_distance = (total_trip_distance_batch + total_trip_distance_speed) / (total_rides_batch + total_rides_speed)

  return average_distance

average_distance = average_trip_distance()

# dashboard title
st.title("Real-Time NYC Taxi Dashboard")

# creating a single-element container
placeholder = st.empty()

# near real-time / live feed simulation
while True:
  with placeholder.container():

    # create three columns
    kpi1, kpi2 = st.columns(2)

    # fill in those three columns with respective metrics or KPIs
    kpi1.metric(
        label="Average Trip Distance",
        value=round(average_distance, 3),
    )
        
    kpi2.metric(
        label="Average Total Amount",
        value=int(0),
        delta=-10 + 00,
    )

        # # create two columns for charts
        # fig_col1, fig_col2 = st.columns(2)
        # with fig_col1:
        #     st.markdown("### First Chart")
        #     fig = px.density_heatmap(
        #         data_frame=df, y="age_new", x="marital"
        #     )
        #     st.write(fig)
            
        # with fig_col2:
        #     st.markdown("### Second Chart")
        #     fig2 = px.histogram(data_frame=df, x="age_new")
        #     st.write(fig2)

    time.sleep(15)
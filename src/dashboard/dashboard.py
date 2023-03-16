import time  
import numpy as np  
import pandas as pd  
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px 
import streamlit as st  
from datetime import datetime
from cassandra.cluster import Cluster

st.set_page_config(
  page_title="Real-Time NYC Taxi Dashboard",
  page_icon="✅",
  layout="wide",
)

session = Cluster().connect()
session.set_keyspace("nyc_taxi")

def get_average_trip_distance():
  batch_query = '''
                  SELECT total_trip_distance, total_rides, ended_at, started_at
                  FROM total_trip_distance_batch 
                  LIMIT 1 ;
                '''
  batch_data = session.execute(batch_query).all()
  total_trip_distance_batch, total_rides_batch, ended_at, started_at  = batch_data[0]

  speed_query = f'''
                  SELECT SUM(trip_distance) as total_trip_distance, 
                         COUNT(*) as total_rides 
                  FROM total_trip_distance_speed 
                  WHERE created_at >= '{started_at}' 
                    AND created_at <= '{datetime.now()}' 
                  ALLOW FILTERING ;
                '''
  speed_data = session.execute(speed_query).all()
  total_trip_distance_speed, total_rides_speed = speed_data[0]

  average_distance = (total_trip_distance_batch + total_trip_distance_speed) / (total_rides_batch + total_rides_speed)

  return average_distance

def get_average_total_amount():
  batch_query = '''
                  SELECT total_amount, total_rides, ended_at, started_at
                  FROM total_amount_batch 
                  LIMIT 1 ;
                '''
  batch_data = session.execute(batch_query).all()
  total_amount_batch, total_rides_batch, ended_at, started_at  = batch_data[0]

  speed_query = f'''
                  SELECT SUM(total_amount) as total_amount, 
                         COUNT(*) as total_rides 
                  FROM total_amount_speed 
                  WHERE created_at >= '{started_at}' 
                    AND created_at <= '{datetime.now()}' 
                  ALLOW FILTERING ;
                '''
  speed_data = session.execute(speed_query).all()
  total_amount_speed, total_rides_speed = speed_data[0]

  average_total_amount = (total_amount_batch + total_amount_speed) / (total_rides_batch + total_rides_speed)

  return average_total_amount

def get_total_passenger():
  batch_query = '''
                  SELECT total_passenger, total_rides, ended_at, started_at 
                  FROM total_passenger_batch 
                  LIMIT 1 ;
                '''
  batch_data = session.execute(batch_query).all()
  total_passenger_batch, total_rides_batch, ended_at, started_at  = batch_data[0]

  speed_query = f'''
                  SELECT SUM(passenger_count) as total_passenger, 
                         COUNT(*) as total_rides 
                  FROM total_passenger_speed 
                  WHERE created_at >= '{started_at}' 
                    AND created_at <= '{datetime.now()}' 
                  ALLOW FILTERING ;
                '''
  speed_data = session.execute(speed_query).all()
  total_passenger_speed, total_rides_speed = speed_data[0]

  average_passenger = total_passenger_batch + total_passenger_speed

  return average_passenger

def get_top10_pickup_districts():
  speed_query = f'''
                  SELECT vendor_id, 
                         pu_location_id 
                  FROM pickup_dropoff_speed ;
                '''
  speed_data = session.execute(speed_query)
  pickup_df = pd.DataFrame(speed_data)

  top10_pickup_districts_df = pickup_df \
                              .groupby('pu_location_id', as_index=False) \
                              .count() \
                              .rename({'vendor_id' : 'count'}, axis=1) \
                              .sort_values('count', ascending=False) \
                              .head(10)

  taxi_zone_lookup_df = pd.read_csv("./data_source/taxi_zone_lookup.csv")
  top10_pickup_districts_df = top10_pickup_districts_df.merge(
          taxi_zone_lookup_df,
          left_on='pu_location_id',
          right_on='LocationID',
          how='left'
  )

  return top10_pickup_districts_df

def get_top10_dropoff_districts():
  speed_query = f'''
                  SELECT vendor_id, 
                         do_location_id 
                  FROM pickup_dropoff_speed ;
                '''
  speed_data = session.execute(speed_query)
  dropoff_df = pd.DataFrame(speed_data)

  top10_dropoff_districts_df = dropoff_df \
                              .groupby('do_location_id', as_index=False) \
                              .count() \
                              .rename({'vendor_id' : 'count'}, axis=1) \
                              .sort_values('count', ascending=False) \
                              .head(10)

  taxi_zone_lookup_df = pd.read_csv("./data_source/taxi_zone_lookup.csv")
  top10_dropoff_districts_df = top10_dropoff_districts_df.merge(
          taxi_zone_lookup_df,
          left_on='do_location_id',
          right_on='LocationID',
          how='left'
  )

  return top10_dropoff_districts_df

def get_user_per_payment():
  speed_query = f'''
                  SELECT vendor_id, 
                         payment_type 
                  FROM user_per_payment_speed ;
                '''
  speed_data = session.execute(speed_query)
  df = pd.DataFrame(speed_data)

  user_per_payment_df = df \
                          .groupby('payment_type', as_index=False) \
                          .sum() \
                          .rename({'vendor_id' : 'count'}, axis=1) \
                          .sort_values('count', ascending=False)
  payment_type = ["Credit card", "Cash", "No charge", "Dispute", "Unknown", "Voided trip"]
  user_per_payment_df['payment_name'] = payment_type[:user_per_payment_df.shape[0]]

  return user_per_payment_df

# dashboard title
st.title("Real-Time NYC Taxi Dashboard")

# creating a single-element container
placeholder = st.empty()

# near real-time / live feed simulation
while True:
  with placeholder.container():
    average_distance = get_average_trip_distance()
    average_total_amount = get_average_total_amount()
    total_passenger = get_total_passenger()

    # create three columns
    kpi1, kpi2, kpi3 = st.columns(3)

    kpi1.metric(
      label="Average Trip Distance",
      value=round(average_distance, 3)
    )
        
    kpi2.metric(
      label="Average Total Amount",
      value=round(average_total_amount, 3)
    )

    kpi3.metric(
      label="Total Passenger",
      value=round(total_passenger, 2)
    )

    # create two columns for charts
    fig_col1, fig_col2 = st.columns(2)
    with fig_col1:
      st.markdown("Top 10 Pickup and Dropoff Places")

      fig = plt.figure(figsize=(20, 15))
      ax0 = fig.add_subplot(1, 2, 1)
      ax1 = fig.add_subplot(1, 2, 2)

      sns.barplot(
          data=get_top10_pickup_districts(),
          x='count',
          y='Zone',
          color='cornflowerblue',
          edgecolor='black',
          ax=ax0
      )

      sns.barplot(
          data=get_top10_dropoff_districts(),
          x='count',
          y='Zone',
          color='cornflowerblue',
          edgecolor='black',
          ax=ax1
      )
      ax1.invert_xaxis()
      ax1.yaxis.tick_right()
      ax1.yaxis.set_label_position('right')

      ax0.grid(color='black', linestyle=':', axis='x', dashes=(1,5), alpha=0.5)
      ax1.grid(color='black', linestyle=':', axis='x', dashes=(1,5), alpha=0.5)

      ax0.set_title('PICK UP PLACES')
      ax1.set_title('DROP OFF PLACES')

      ax0.set_ylabel('')
      ax1.set_ylabel('')
      ax0.set_xlabel('')
      ax1.set_xlabel('')

      for s in ['top', 'bottom', 'right']:
          ax0.spines[s].set_visible(False)
          
      for s in ['top', 'bottom','left']:    
          ax1.spines[s].set_visible(False)
      
      session.execute("TRUNCATE TABLE pickup_dropoff_speed;")
      
      st.write(fig)
            
    with fig_col2:
      st.markdown("User per payment")
      
      fig = px.pie(get_user_per_payment(), 
                   values='count', 
                   names='payment_name', 
                   title='User per payment')

      session.execute("TRUNCATE TABLE user_per_payment_speed;")

      st.write(fig)

    time.sleep(45)
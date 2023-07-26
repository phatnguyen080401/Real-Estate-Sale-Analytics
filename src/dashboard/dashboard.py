import os
import time  
import numpy as np  
import pandas as pd  
import plotly.express as px 
import streamlit as st  
from datetime import datetime
from dotenv import load_dotenv

import snowflake.connector
from snowflake.connector import errors, errorcode

load_dotenv(os.path.join("..", "..", "deploy", ".env"))

st.set_page_config(
  page_title="Real Estate Sales Dashboard",
  page_icon="✅",
  layout="wide",
)

config = {
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "region": os.getenv('SNOWFLAKE_REGION'),
    "database": os.getenv('SNOWFLAKE_DATABASE'),
    "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
    "role": os.getenv('SNOWFLAKE_ROLE')
}

def query_total_sale_amount(conn_config):
  try:
    conn = snowflake.connector.connect(**conn_config)
    cursor = conn.cursor()

    query_batch = '''
        SELECT 
          total_sale_amount, 
          started_at
        FROM sale_batch.total_sale_amount_ratio
        ORDER BY ended_at DESC
        LIMIT 1
    '''
    total_sale_amount_1, started_at = cursor.execute(query_batch).fetchone()

    query_speed = f'''
        SELECT 
          SUM(total_sale_amount)
        FROM sale_speed.total_sale_amount_ratio
        WHERE 
          created_at >= '{started_at}' 
          AND created_at <= '{datetime.now()}' 
    '''
    total_sale_amount_2 = cursor.execute(query_speed).fetchone()[0]

    total_sale_amount = total_sale_amount_1 + total_sale_amount_2

    return total_sale_amount
  except errors.Error as err:
    if err.errno == errorcode.ER_FAILED_TO_CONNECT_TO_DB:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_NO_ACCOUNT_NAME:
      print("Missing account name")
    elif err.errno == errorcode.ER_NO_USER:
      print("Missing username")
    elif err.errno == errorcode.ER_NO_PASSWORD:
      print("Missing password")
    else:
      print(err)
  else:
    conn.close()

def query_total_customer(conn_config):
  try:
    conn = snowflake.connector.connect(**conn_config)
    cursor = conn.cursor()

    query_batch = '''
        SELECT 
          total_customer, 
          started_at
        FROM sale_batch.total_sale_amount_ratio
        ORDER BY ended_at DESC
        LIMIT 1
    '''
    total_customer_1, started_at = cursor.execute(query_batch).fetchone()

    query_speed = f'''
        SELECT 
          SUM(total_customer)
        FROM sale_speed.total_sale_amount_ratio
        WHERE 
          created_at >= '{started_at}' 
          AND created_at <= '{datetime.now()}' 
    '''
    total_customer_2 = cursor.execute(query_speed).fetchone()[0]

    total_customer = total_customer_1 + total_customer_2

    return total_customer
  except errors.Error as err:
    if err.errno == errorcode.ER_FAILED_TO_CONNECT_TO_DB:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_NO_ACCOUNT_NAME:
      print("Missing account name")
    elif err.errno == errorcode.ER_NO_USER:
      print("Missing username")
    elif err.errno == errorcode.ER_NO_PASSWORD:
      print("Missing password")
    else:
      print(err)
  else:
    conn.close()

def query_average_sale_ratio(conn_config):
  try:
    conn = snowflake.connector.connect(**conn_config)
    cursor = conn.cursor()

    query_batch = '''
        SELECT 
          total_sales_ratio,
          total_customer,
          started_at
        FROM sale_batch.total_sale_amount_ratio
        ORDER BY ended_at DESC
        LIMIT 1
    '''
    total_sales_ratio_1, total_customer_1, started_at = cursor.execute(query_batch).fetchone()

    query_speed = f'''
        SELECT 
          SUM(total_sales_ratio),
          SUM(total_customer)
        FROM sale_speed.total_sale_amount_ratio
        WHERE 
          created_at >= '{started_at}' 
          AND created_at <= '{datetime.now()}' 
    '''
    total_sales_ratio_2, total_customer_2 = cursor.execute(query_speed).fetchone()

    avg_sale_ratio = (total_sales_ratio_1 + total_sales_ratio_2) / (total_customer_1 + total_customer_2)

    return avg_sale_ratio
  except errors.Error as err:
    if err.errno == errorcode.ER_FAILED_TO_CONNECT_TO_DB:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_NO_ACCOUNT_NAME:
      print("Missing account name")
    elif err.errno == errorcode.ER_NO_USER:
      print("Missing username")
    elif err.errno == errorcode.ER_NO_PASSWORD:
      print("Missing password")
    else:
      print(err)
  else:
    conn.close()

def query_total_customer_by_property_type(conn_config):
  try:
    conn = snowflake.connector.connect(**conn_config)
    cursor = conn.cursor()

    query_batch = '''
        SELECT 
          property_type,
          SUM(total_customer),
          started_at
        FROM sale_batch.total_customer_by_property_type
        GROUP BY 
          property_type, 
          started_at;
    '''
    df_1 = cursor.execute(query_batch).fetch_pandas_all()

    started_at = df_1.iloc[0, 2]

    query_speed = f'''
        SELECT 
          property_type,
          SUM(total_customer)
        FROM sale_speed.total_customer_by_property_type
        WHERE 
          created_at >= '{started_at}' 
          AND created_at <= '{datetime.now()}'
        GROUP BY property_type;
    '''
    df_2 = cursor.execute(query_speed).fetch_pandas_all()

    merge_df = pd.concat([df_1.iloc[:, :2], df_2])
    merge_df = merge_df.rename(columns={
        merge_df.columns[0]: "property_type",
        merge_df.columns[1]: "total_customer"
    })

    total_customer_by_property_type_df = merge_df \
                                              .groupby("property_type") \
                                              .sum() \
                                              .reset_index()

    return total_customer_by_property_type_df
  except errors.Error as err:
    if err.errno == errorcode.ER_FAILED_TO_CONNECT_TO_DB:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_NO_ACCOUNT_NAME:
      print("Missing account name")
    elif err.errno == errorcode.ER_NO_USER:
      print("Missing username")
    elif err.errno == errorcode.ER_NO_PASSWORD:
      print("Missing password")
    else:
      print(err)
  else:
    conn.close()

def query_total_customer_by_town(conn_config):
  try:
    conn = snowflake.connector.connect(**conn_config)
    cursor = conn.cursor()

    query_batch = '''
        SELECT 
          town,
          SUM(total_customer),
          started_at
        FROM sale_batch.total_customer_by_town
        GROUP BY 
          town, 
          started_at;
    '''
    df_1 = cursor.execute(query_batch).fetch_pandas_all()

    started_at = df_1.iloc[0, 2]

    query_speed = f'''
        SELECT 
          town,
          SUM(total_customer)
        FROM sale_speed.total_customer_by_town
        WHERE 
          created_at >= '{started_at}' 
          AND created_at <= '{datetime.now()}'
        GROUP BY town;
    '''
    df_2 = cursor.execute(query_speed).fetch_pandas_all()

    merge_df = pd.concat([df_1.iloc[:, :2], df_2])
    merge_df = merge_df.rename(columns={
        merge_df.columns[0]: "town",
        merge_df.columns[1]: "total_customer"
    })

    total_customer_by_town_df = merge_df \
                                  .groupby("town") \
                                  .sum() \
                                  .reset_index()

    return total_customer_by_town_df
  except errors.Error as err:
    if err.errno == errorcode.ER_FAILED_TO_CONNECT_TO_DB:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_NO_ACCOUNT_NAME:
      print("Missing account name")
    elif err.errno == errorcode.ER_NO_USER:
      print("Missing username")
    elif err.errno == errorcode.ER_NO_PASSWORD:
      print("Missing password")
    else:
      print(err)
  else:
    conn.close()

# dashboard title
st.markdown("<h1 style='text-align: center;'>Real-Time Estate Sales Dashboard</h1>", unsafe_allow_html=True)

# creating a single-element container
placeholder = st.empty()

# near real-time / live feed simulation
while True:
  with placeholder.container():
    total_sale_amount = query_total_sale_amount(config)
    total_customer = query_total_customer(config)
    average_sale_ratio = query_average_sale_ratio(config)

    # create three columns
    kpi1, kpi2, kpi3 = st.columns(3)

    kpi1.metric(
      label="Total Sale Amount",
      value=f"{round(total_sale_amount / 10**9, 2)} Billion $"
    )
        
    kpi2.metric(
      label="Total Customer",
      value=round(total_customer, 0)
    )

    kpi3.metric(
      label="Avg Sale Ratio",
      value=round(average_sale_ratio, 2)
    )

    # create two columns for charts
    fig_col1, fig_col2 = st.columns(2)
    with fig_col1:
      st.markdown("Customer per property type")

      customer_per_property_type_df = query_total_customer_by_property_type(config)

      fig = px.pie(
                    customer_per_property_type_df, 
                    values='total_customer', 
                    names='property_type'
                  )
            
      st.write(fig)
            
    with fig_col2:
      st.markdown("Customer per town")
      
      customer_per_town_df = query_total_customer_by_town(config)
      top_10_town_df = customer_per_town_df \
                          .sort_values(by="total_customer", ascending=False) \
                          .head(10)

      fig = px.bar(
                    top_10_town_df, 
                    x='town', 
                    y='total_customer'
                  )

      st.write(fig)

    time.sleep(60*2)
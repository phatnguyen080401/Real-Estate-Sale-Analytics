#%%
import sys
sys.path.append("..")
import time
import pandas as pd
import math
import numpy as np
import tensorflow as tf
import matplotlib.pyplot as plt
import seaborn as sns
import joblib

import snowflake.connector
from snowflake.connector import errors, errorcode
from termcolor import colored as cl # text customization
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, make_scorer
from sklearn.linear_model import LinearRegression # OLS algorithm
from sklearn.linear_model import Ridge # Ridge algorithm
from sklearn.linear_model import Lasso # Lasso algorithm
from sklearn.linear_model import BayesianRidge # Bayesian algorithm
from sklearn.linear_model import ElasticNet # ElasticNet algorithm
from sklearn.metrics import explained_variance_score as evs # evaluation metric
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from keras.models import load_model

import configparser
#from config.config import config
from logger.logger import Logger

#%%
config = configparser.ConfigParser()
config.read('/mnt/c/Users/Tuan/Desktop/Mix/NYC-Taxi-Analytics/src/config.ini')
configure = {
    "user": config['SNOWFLAKE']['USER'],
    "password": config['SNOWFLAKE']['PASSWORD'],
    "account": config['SNOWFLAKE']['ACCOUNT'],
    "region": config['SNOWFLAKE']['REGION'],
    "database": config['SNOWFLAKE']['DATABASE'],
    "warehouse": config['SNOWFLAKE']['WAREHOUSE'],
    "role": config['SNOWFLAKE']['ROLE']
}

#logger = Logger('Get-Train-Data')

class GetTrainData:
    def __init__(self):
        self._spark = SparkSession \
                .builder \
                .master("local[*]") \
                .appName("Batch-Total-Customer-By-Property-Type") \
                .config("spark.jars.packages", 
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0," +
                        "net.snowflake:snowflake-jdbc:3.13.14," + 
                        "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.2"
                    ) \
                .getOrCreate()
        
        self._spark.sparkContext.setLogLevel("ERROR")

    # def get(self):
    #     try:
    #         df = self._spark \
    #                     .read \
    #                     .format("snowflake") \
    #                     .options(**SNOWFLAKE_OPTIONS) \
    #                     .option("sfSchema", "sale_lake") \
    #                     .option("dbtable", "data_lake") \
    #                     .load()
            
    #         logger.info(f"Read data from table sale_lake.data_lake")

    #         return df
    #     except Exception as e:
    #         logger.error(e)

def pre_processing(df):
        # Replace with the most occurrent value and turn to dummy values.
        property_categories = ['Commercial', 'Residential', 'Vacant Land', 'Apartments', 'Industrial', 'Public Utility', 'Condo', 'Two Family', 'Three Family', 'Single Family', 'Four Family']

        for i in range(len(property_categories)):
            property_categories[i] = 'Property_' + property_categories[i]

        residential_categories = ['Single Family', 'Condo', 'Two Family', 'Three Family', 'Four Family']

        for i in range(len(residential_categories)):
            residential_categories[i] = 'Residential_' + residential_categories[i]

        df['PROPERTY_TYPE'] = df['PROPERTY_TYPE']\
            .fillna(df['PROPERTY_TYPE'].mode().iloc[0])
        df['RESIDENTIAL_TYPE'] = df['RESIDENTIAL_TYPE']\
            .fillna(df['RESIDENTIAL_TYPE'].mode().iloc[0])

        dummy_property_df = pd.get_dummies(df['PROPERTY_TYPE'], prefix='Property')
        dummy_property_df = dummy_property_df.reindex(columns=property_categories, fill_value=0)

        dummy_residential_df = pd.get_dummies(df['RESIDENTIAL_TYPE'], prefix='Residential')
        dummy_residential_df = dummy_residential_df.reindex(columns=residential_categories, fill_value=0)
        
        df = pd.concat([df, dummy_property_df, dummy_residential_df], axis=1)

        df.dropna()
        df = df[df['SALES_RATIO'] > 0.3]
        df = df[df['SALES_RATIO'] < 1.3]

        #Scale the values
        town_encoded_scaler = StandardScaler()
        assessed_prices_scaler = StandardScaler()
        year_scaler = StandardScaler()
        
        df['ASSESSED_VALUE'] = assessed_prices_scaler\
            .fit_transform(df['ASSESSED_VALUE'].values.reshape(-1,1))
        df['LIST_YEAR'] = year_scaler\
            .fit_transform(df['LIST_YEAR'].values.reshape(-1,1))
        # Using target encoding method to handle Town feature
        town_avg_price = df\
            .groupby('TOWN')['SALE_AMOUNT'].mean()
        df['TOWN_ENCODED'] = df['TOWN']\
            .map(town_avg_price)
        df['TOWN_ENCODED'] = town_encoded_scaler.fit_transform(df['TOWN_ENCODED'].values.reshape(-1,1))
        drop_columns = ['TOWN','SERIAL_NUMBER','DATE_RECORDED', 'ADDRESS', 'SALES_RATIO', 'PROPERTY_TYPE', 'RESIDENTIAL_TYPE', 'NON_USE_CODE', 'ASSESSOR_REMARKS', 'OPM_REMARKS', 'LOCATION', 'CREATED_AT']

        df = df.drop(columns=drop_columns)
        return df

def get_new_data(conn_config, lasted_sample_date):
    try:
        conn = snowflake.connector.connect(**conn_config)
        cursor = conn.cursor()

        query = f'''
            SELECT
                *
            FROM sale_lake.data_lake
            WHERE
                created_at > '{lasted_sample_date}'
                AND created_at <= '{datetime.now()}'
        '''

        df = cursor.execute(query).fetchall()
        column_names = [column[0] for column in cursor.description]
        df= pd.DataFrame(df, columns=column_names)
        return df
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

def create_model():
    models = []
    models.append(('Linear Regression', LinearRegression()))
    models.append(('Rigde', Ridge()))
    models.append(('Lasso', Lasso()))
    models.append(('Bayesian', BayesianRidge()))
    models.append(('Elastic Net', ElasticNet()))  

    return models      
def mape_score(y_true, y_pred):
        return -np.mean(np.abs((y_true - y_pred) / y_true)) * 100

def modeling(x_train, y_train, x_val, y_val):
    models = create_model()

    print(cl('EXPLAINED VARIANCE AND MAPE SCORE:', attrs = ['bold']))
    print('-------------------------------------------------------------------------------')
    
    scorer = make_scorer(mape_score, greater_is_better=False)
    vari = []
    mape = []
    cross_validate = []
    models_name = []
    for name, model in models:
        models_name.append(name)
        model.fit(x_train, y_train)
        predicted = model.predict(x_val)
        vari.append(evs(y_val, predicted))
        mape.append(mean_absolute_percentage_error(y_val, predicted))
        print('-------------------------------------------------------------------------------')
        print(cl('Explained Variance Score of {} model is {}'.format(name, evs(y_val, predicted)), attrs = ['bold']))
        print(cl('MAPE of {} model is {}'.format(name, mean_absolute_percentage_error(y_val, predicted)), attrs = ['bold']))

    print('Cross Validate')
    for name, model in models:
        cross = cross_val_score(model, X_train, y_train,cv=5, scoring=scorer).mean()
        cross_validate.append(cross)
        print(cl('Cross-validated MAPE of {} model is {}'.format(name, cross), attrs = ['bold']))

    return vari, mape, cross_validate, models_name
#%%
if __name__ == '__main__':
        # df = GetTrainData().get()
        # df = df.toPandas()
        df = pd.read_csv('Real_Estate_Sales_2001-2020_GL.csv')
        df = df[200000:400000]
        df.columns = ['SERIAL_NUMBER','LIST_YEAR', 'DATE_RECORDED', 'TOWN','ADDRESS', 'ASSESSED_VALUE','SALE_AMOUNT','SALES_RATIO', 'PROPERTY_TYPE', 'RESIDENTIAL_TYPE', 'NON_USE_CODE', 'ASSESSOR_REMARKS', 'OPM_REMARKS', 'LOCATION']
        df['CREATED_AT'] = 0
        df = df[df['SALE_AMOUNT']<1500000]
        lasted_sample_date = df['CREATED_AT'].max()
        with open("lasted_sample_date.txt", "w") as file:
        # Write data to the file
            file.write(f'{lasted_sample_date}')

        df = pre_processing(df)
        X_full = df.loc[:,df.columns != 'SALE_AMOUNT'].values
        y_full = df['SALE_AMOUNT'].values

        X_train, X_test, y_train, y_test = train_test_split(X_full, y_full, test_size = 0.15, random_state = 0)
        
        vari, mape, cross, models_name = modeling(X_train, y_train, X_test, y_test)
        vari = [float('%.4f' % (va * 100)) for va in vari]
        mape = [float('%.4f' % (ma * 100)) for ma in mape]
        cross = [float('%.4f' % cro) for cro in cross]

        ax = sns.barplot(x=models_name, y=vari)
        for i, v in enumerate(vari):
            ax.text(i, v, str(v), ha='center', va='bottom')
        # Set labels and title
        plt.ylim(0, 100)
        plt.xlabel('Models')
        plt.ylabel('Values (%)')
        plt.title('Explained Variance Score')

        param_grid = {
            'alpha': [1e-08, 0.001, 0.01, 1, 5, 10,
                                   20, 30, 35, 40, 45, 50, 55, 100, 200],  # Values for the alpha hyperparameter
            'max_iter': [1000, 2000, 3000, 4000],  # Values for the max_iter hyperparameter
            'selection': ['cyclic', 'random'],  # Values for the selection criterion hyperparameter
            'warm_start': [True, False],
            'precompute': [False, True]
        }
        lasso = Lasso()
        scorer = make_scorer(mape_score, greater_is_better=False)

        grid_search = GridSearchCV(estimator=lasso, param_grid=param_grid, cv=5, scoring=scorer)
        grid_search.fit(X_train, y_train)

        print("Best Hyperparameters:", grid_search.best_params_)
        print("Best Mean Absolute Percentage Error:", -grid_search.best_score_)

        lasso = Lasso(alpha=100, max_iter=1000, precompute=False, selection='random', warm_start=False)
        lasso.fit(X_train, y_train)

        y_pred = lasso.predict(X_test)
        mape_pre = mean_absolute_percentage_error(y_true=y_test, y_pred=y_pred)

        joblib.dump(lasso, 'lasso_model.sav')

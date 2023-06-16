#%%
import sys
sys.path.append("..")
import time
import pandas as pd
import math
import numpy as np
import tensorflow as tf
import pickle
import matplotlib.pyplot as plt

import snowflake.connector
from snowflake.connector import errors, errorcode

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split
from keras.models import Model, load_model
from keras.layers import Dense, Dropout, Flatten, Embedding, TextVectorization, concatenate, BatchNormalization
from keras import Input
from keras.optimizers import Adam
from keras.callbacks import EarlyStopping

from config.config import config
from logger.logger import Logger

#%%
SNOWFLAKE_OPTIONS = {
    "sfURL" : config['SNOWFLAKE']['URL'],
    "sfAccount": config['SNOWFLAKE']['ACCOUNT'],
    "sfUser" : config['SNOWFLAKE']['USER'],
    "sfPassword" : config['SNOWFLAKE']['PASSWORD'],
    "sfDatabase" : config['SNOWFLAKE']['DATABASE'],
    "sfWarehouse" : config['SNOWFLAKE']['WAREHOUSE']
}

configure = {
    "user": config['SNOWFLAKE']['USER'],
    "password": config['SNOWFLAKE']['PASSWORD'],
    "account": config['SNOWFLAKE']['ACCOUNT'],
    "region": config['SNOWFLAKE']['REGION'],
    "database": config['SNOWFLAKE']['DATABASE'],
    "warehouse": config['SNOWFLAKE']['WAREHOUSE'],
    "role": config['SNOWFLAKE']['ROLE']
}

logger = Logger('Get-Train-Data')

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

    def get(self):
        try:
            df = self._spark \
                        .read \
                        .format("snowflake") \
                        .options(**SNOWFLAKE_OPTIONS) \
                        .option("sfSchema", "sale_lake") \
                        .option("dbtable", "data_lake") \
                        .load()
            
            logger.info(f"Read data from table sale_lake.data_lake")

            return df
        except Exception as e:
            logger.error(e)

def pre_processing(df, new=False):
        # Fill null value to None-Property and None-Residential.
        property_categories = ['Commercial', 'Residential', 'Vacant Land', 'Apartments', 'Industrial', 'Public Utility', 'Condo', 'Two Family', 'Three Family', 'Single Family', 'Four Family', 'None-Property']

        for i in range(len(property_categories)):
            property_categories[i] = 'Property_' + property_categories[i]

        residential_categories = ['Single Family', 'Condo', 'Two Family', 'Three Family', 'Four Family', 'None-Residential']

        for i in range(len(residential_categories)):
            residential_categories[i] = 'Residential_' + residential_categories[i]

        df['PROPERTY_TYPE'] = df['PROPERTY_TYPE'].fillna('None-Property')
        df['RESIDENTIAL_TYPE'] = df['RESIDENTIAL_TYPE'].fillna('None-Residential')

        dummy_property_df = pd.get_dummies(df['PROPERTY_TYPE'], prefix='Property')
        dummy_property_df = dummy_property_df.reindex(columns=property_categories, fill_value=0)

        dummy_residential_df = pd.get_dummies(df['RESIDENTIAL_TYPE'], prefix='Residential')
        dummy_residential_df = dummy_residential_df.reindex(columns=residential_categories, fill_value=0)
        
        df = pd.concat([df, dummy_property_df, dummy_residential_df], axis=1)

        df.dropna()
        df = df[df['SALES_RATIO'] > 0.3]
        df = df[df['SALES_RATIO'] < 1.3]

        drop_columns = ['SERIAL_NUMBER', 'DATE_RECORDED', 'ADDRESS', 'SALES_RATIO', 'PROPERTY_TYPE', 'RESIDENTIAL_TYPE', 'NON_USE_CODE', 'ASSESSOR_REMARKS', 'OPM_REMARKS', 'LOCATION', 'CREATED_AT']

        df = df.drop(columns=drop_columns)

        #prices_scaler = StandardScaler()
        if new == True:
            assessed_prices_scaler = StandardScaler()
            years_scaler = StandardScaler()

            #df['SALE_AMOUNT'] = prices_scaler.fit_transform(df['SALE_AMOUNT'].values.reshape(-1,1))
            df['ASSESSED_VALUE'] = assessed_prices_scaler.fit_transform(df['ASSESSED_VALUE'].values.reshape(-1,1))
            df['LIST_YEAR'] = years_scaler.fit_transform(df['LIST_YEAR'].values.reshape(-1,1))

            with open('assessed_scaler.pkl', 'wb') as file:
                pickle.dump(assessed_prices_scaler, file)

            with open('year_scaler.pkl', 'wb') as file:
                pickle.dump(years_scaler, file)

            return df
        else:
            with open('./model/assessed_scaler.pkl', 'rb') as file:
                assessed_prices_scaler = pickle.load(file)

            with open('./model/year_scaler.pkl', 'rb') as file:
                years_scaler = pickle.load(file)

            df['ASSESSED_VALUE'] = assessed_prices_scaler.transform(df['ASSESSED_VALUE'].values.reshape(-1,1))
            df['LIST_YEAR'] = years_scaler.transform(df['LIST_YEAR'].values.reshape(-1,1))

            return df

def create_model(neurons, num_layers, dropout_rate):
    def find_max_len(vocab):
        max_len = 0
        for string in vocab:
            if max_len < len(string.split()):
                max_len = len(string.split())
        return max_len

    town_vocab = np.unique(df['TOWN'])
    town_vec_dim = int(math.sqrt(len(town_vocab)))
    town_max_len = find_max_len(town_vocab)
    town_text_vectorizer = TextVectorization(max_tokens=len(town_vocab) + 1, output_mode="int", output_sequence_length=town_max_len)
    town_text_vectorizer.adapt(town_vocab)

    # Create NN model
    num_inputs = Input(shape=(20), dtype=tf.float32)

    town_inputs = Input(shape=(1), dtype=tf.string)
    town_embedding_layer = Embedding(input_dim=town_text_vectorizer.vocabulary_size() + 1, input_length=town_max_len, output_dim=town_vec_dim, name='Town_Embedding_Layer')
    # Preprocess the string inputs, turning them into int sequences
    town_sequences = town_text_vectorizer(town_inputs)
    town_embed = town_embedding_layer(town_sequences)
    town_flatten = Flatten()(town_embed)

    concaten = concatenate([num_inputs, town_flatten])

    batch_norm1 = BatchNormalization()(concaten)

    previous_layer = batch_norm1

    for _ in range(num_layers):
        hidden_layer = Dense(neurons, activation='relu')(previous_layer)
        drop_out = Dropout(dropout_rate)(hidden_layer)
        previous_layer = drop_out

    output = Dense(1)(previous_layer)

    model = Model(inputs=[num_inputs, town_inputs], outputs=[output])

    return model

def hyperpara_tunning(num_neurons, num_layers, droprates, batch_size_list, epochs_list):
    models = []
    best_mae = 1
    best_model_index = 0
    for neuron in num_neurons:
        for layer in num_layers:
            for droprate in droprates:
                models.append(create_model(neuron, layer, droprate))
    models_fresh = models.copy()
    for index, model in enumerate(models):
        model.compile(loss='mse', optimizer=Adam(learning_rate=1e-3), metrics=['accuracy'])
        early_stop = EarlyStopping(monitor='val_loss', patience=4)
        history = model.fit(data_train, np.asarray(y_train).astype(np.float32), epochs=10, 
                            validation_split=0.2,
                            batch_size= 16,
                            callbacks=[early_stop])
        
        mae = mean_absolute_error(y_test, model.predict(data_test))
        print(mae)
        if mae < best_mae:
            best_mae = mae
            best_model_index = index

    models_fresh[best_model_index].save('./model/best_fresh_model')

    best_epochs = 10
    best_batch_size = 16
    
    for batchsize in batch_size_list:
        for epochs in epochs_list:
            model = load_model('./model/best_fresh_model')
            model.compile(loss='mse', optimizer=Adam(learning_rate=1e-3), metrics=['accuracy'])
            early_stop = EarlyStopping(monitor='val_loss', patience=4)
            history = model.fit(data_train, np.asarray(y_train).astype(np.float32), epochs=epochs, 
                                validation_split=0.2,
                                batch_size= batchsize,
                                callbacks=[early_stop])
            mae = mean_absolute_error(y_test, model.predict(data_test))
            if mae < best_mae:
                best_mae = mae
                best_epochs = epochs
                best_batch_size = batchsize

    return models_fresh[best_model_index], best_epochs, best_batch_size

def save_model(model, best_batch_size, best_epochs):
    model.compile(loss='mse', optimizer=Adam(learning_rate=1e-3), metrics=['accuracy'])
    early_stop = EarlyStopping(monitor='val_loss', patience=4)
    history = model.fit(data_train, np.asarray(y_train).astype(np.float32), epochs=best_epochs, 
                        validation_split=0.2,
                        batch_size= best_batch_size,
                        callbacks=[early_stop])
    model.save('./model/best_model')

def model_input_data(df, new=True):
    x_full = df.iloc[:,df.columns != 'SALE_AMOUNT'].values
    y_full = df.iloc[:,df.columns == 'SALE_AMOUNT'].values
    if new == True:
        x_train, x_test, y_train, y_test = train_test_split(x_full, y_full, test_size=0.2, shuffle=True, random_state=40)
        
        data_train = (np.asarray(np.c_[x_train[:,0], x_train[:,2:]]).astype(np.float32), x_train[:,1].reshape((x_train[:,1].shape[0],1)))
        data_test = (np.asarray(np.c_[x_test[:,0], x_test[:,2:]]).astype(np.float32), x_test[:,1].reshape((x_test[:,1].shape[0],1)))
        return data_train, y_train, data_test, y_test
    else:
        data_train = (np.asarray(np.c_[x_full[:,0], x_full[:,2:]]).astype(np.float32), x_full[:,1].reshape((x_full[:,1].shape[0],1)))

        return data_train, y_full

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
#%%
if __name__ == '__main__':
    new = 1
    if new == 0:
        df = GetTrainData().get()
        df = df.toPandas()

        lasted_sample_date = df['CREATED_AT'].max()
        with open("./model/lasted_sample_date.txt", "w") as file:
        # Write data to the file
            file.write(f'{lasted_sample_date}')

        df = pre_processing(df, True)

        data_train, y_train, data_test, y_test = model_input_data(df)

        num_neurons = [20, 30, 40, 50]
        num_layers = [2, 3, 4]
        droprates = [0.1, 0.2]

        batch_size_list = [8, 16, 32, 64]
        epochs_list = [10, 15]
        
        model, best_epochs, best_batch_size = hyperpara_tunning(num_neurons, num_layers, droprates, batch_size_list, epochs_list)
        save_model(model, best_batch_size, best_epochs)
    else:
        with open("./model/lasted_sample_date.txt", "r") as file:
            # Read lines from the file
            lines = file.readlines()
        lasted_sample_date = datetime.strptime(lines[0], "%Y-%m-%d %H:%M:%S.%f")
        model = load_model('./model/best_model')
        while True:
            new_df = get_new_data(configure, lasted_sample_date)
            if len(new_df) > 5000:
                lasted_sample_date = new_df['CREATED_AT'].max()
                
                train_data = new_df.sample(frac=0.9)
                save_data = new_df.sample(frac=0.1)

                save_data.to_csv('./model/data.csv', index=False)
                with open("./model/lasted_sample_date.txt", "w") as file:
                # Write data to the file
                    file.write(f'{lasted_sample_date}')
                train_data = pre_processing(train_data)
                data_train, y_train = model_input_data(train_data, False)
                model.train_on_batch(data_train, np.asarray(y_train).astype(np.float32))
                model.save('./model/best_model')
                print('Learning Complete')
            else:
                print('Skip')
            time.sleep(60*30)
# %%

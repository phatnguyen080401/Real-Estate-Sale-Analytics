#%%
import sys
import pandas as pd
import streamlit as st
import numpy as np
import matplotlib.pyplot as plt

from create_model import pre_processing, model_input_data
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from keras.models import load_model
from matplotlib.ticker import MaxNLocator

st.set_page_config(layout='wide')
dataframe, chart = st.columns(2)

df = pd.read_csv(r'./model/data.csv', nrows=1000)
df = df[df['SALES_RATIO'] > 0.3]
df = df[df['SALES_RATIO'] < 1.3]

gd = GridOptionsBuilder.from_dataframe(df)
# Page divide
gd.configure_pagination(enabled=True)
gd.configure_default_column(editable=True, groupable=True)

gd.configure_selection(selection_mode='single',
                       use_checkbox=True,
                       pre_selected_rows=[0])
girdoptions = gd.build()
with dataframe:
    st.header('New Real Estate Sales GL Data')
    grid_table = AgGrid(df, 
                        gridOptions=girdoptions,
                        update_mode=GridUpdateMode.SELECTION_CHANGED,
                        height=450)

sel_row = grid_table['selected_rows']

# Remove the first key-value pair
data = sel_row[0]
data.pop('_selectedRowNodeInfo')
# Convert to DataFrame
value_df = pd.DataFrame([data], columns=data.keys())

year, price = value_df.iloc[0].to_frame().T.iloc[0]['LIST_YEAR'], value_df.iloc[0].to_frame().T.iloc[0]['SALE_AMOUNT']

for index in range(5):
    new_row = value_df.iloc[0].to_frame().T
    new_row['LIST_YEAR'] = new_row['LIST_YEAR'] + index
    value_df = pd.concat([value_df, new_row.copy()], axis=0)

value_df.reset_index(drop=True, inplace=True)
value_df.drop([0, 1], inplace=True)

print('load model')
model = load_model('./model/best_model')

value_df = pre_processing(value_df)
data_train, _ = model_input_data(value_df, False)

predicted_price = model.predict(data_train)

plot_value = pd.Series(np.insert(predicted_price,0,[price]))
years = list(range(year, year + len(plot_value)))

fig, ax = plt.subplots()
ax.plot(years[0:2], plot_value[0:2], marker='o', color='orange', label='Current Price')
ax.plot(years[1:], plot_value[1:], marker='o', ls='--', color='orange', label='Predicted Price')
# Set x-axis tick labels as integers
ax.xaxis.set_major_locator(MaxNLocator(integer=True))

# Set labels and title
ax.set_xlabel('Year')
ax.set_ylabel('Price (USD)')
ax.set_title('Price in next 4 years')
ax.legend()
# Display the chart using Streamlit
with chart:
    st.header('Predicted Price of Real Estate Sales')
    st.pyplot(fig)



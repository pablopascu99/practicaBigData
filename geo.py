import numpy as np
import streamlit as st
import pandas as pd
from google.cloud import storage
import datetime
from enum import Enum
import os
import json
from streamlit_folium import folium_static
import folium

df = pd.read_csv('C:\\Program Files\\Spark\\PracticaBigData\\datos\\importePorLocalidad.csv',sep = ',')
df.drop(df.columns[[0]], axis=1, inplace=True)
df['CP_CLIENTE'] = df['CP_CLIENTE'].apply(lambda x: '{0:0>5}'.format(x))

with open('almeria_20.json') as f:
  states_topo = json.load(f)

st.write(states_topo['objects']['almeria_wm']['geometries'][0]['properties']['COD_POSTAL'])

m = folium.Map(location=[37.16, -2.33], tiles='Stamen Terrain', zoom_start=9)

folium.Choropleth(
    geo_data=states_topo,
    topojson='objects.almeria_wm',
    name="choropleth",
    line_color='white',
    line_weight=1,
    data=df,
    columns=["CP_CLIENTE", "sum(IMPORTE)"],
    key_on="feature.properties.COD_POSTAL",
    fill_color="YlOrRd",
    fill_opacity=0.6,
    line_opacity=0.4,
    legend_name="Importe total que gasta cada localidad",
).add_to(m)

folium_static(m)
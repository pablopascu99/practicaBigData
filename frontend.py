import numpy as np
import streamlit as st
import plotly.express as plt
import pandas as pd
import datetime
from enum import Enum
import os
import json
from streamlit_folium import folium_static
import folium

df = pd.read_csv('gs://bucket-prueba-nacho/importePorLocalidad.csv',sep = ',', storage_options=({"token": "datos\grandes-volumenes-9d18b4ccbb2f.json"}))
df.drop(df.columns[[0]], axis=1, inplace=True)
df['CP_CLIENTE'] = df['CP_CLIENTE'].apply(lambda x: '{0:0>5}'.format(x))

with open('./datos/almeria_20.json') as f:
  states_topo = json.load(f)

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

datos = pd.read_csv('gs://bucket-prueba-nacho/importeMedioSector.csv', storage_options=({"token": "datos\grandes-volumenes-9d18b4ccbb2f.json"}))

importe = datos['avg(IMPORTE)']
sectores = datos['SECTOR']


figura = plt.pie(names=sectores, values=importe, hole= 0.65, title="<b><i>Importe Medio por Sector</b></i>")
figura.update_traces(textinfo='percent', textposition='outside', hovertemplate = "Sector %{label}: <br>Importe: %{value:.2f}</br> Porcentage: %{percent}")
st.plotly_chart(figura)
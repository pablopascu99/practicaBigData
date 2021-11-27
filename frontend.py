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

pages = {
  "main": "Importe Por Localidad",
  "page1": "Importe Por Sector",
  "page2": "Zona de Comercio",
  "page3": "Ventas en Franjas Horarias",
}
  
selected_page = st.sidebar.radio("Selecciona la página", pages.values())

if selected_page == pages["main"]:
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

elif selected_page == pages["page1"]:

  datos = pd.read_csv('gs://bucket-prueba-nacho/importeMedioSector.csv', storage_options=({"token": "datos\grandes-volumenes-9d18b4ccbb2f.json"}))

  importe = datos['avg(IMPORTE)']
  sectores = datos['SECTOR']

  figura = plt.pie(names=sectores, values=importe, hole= 0.65, title="<b><i>Importe Medio por Sector</b></i>")
  figura.update_traces(textinfo='percent', textposition='outside', hovertemplate = "Sector %{label}: <br>Importe: %{value:.2f}</br> Porcentage: %{percent}")
  st.plotly_chart(figura)

elif selected_page == pages["page3"]:
  sector = st.selectbox("Seleccione un Sector", ['AUTO', 'ALIMENTACION', 'BELLEZA', 'HOGAR', 'MODA Y COMPLEMENTOS', 'OCIO Y TIEMPO LIBRE', 'OTROS', 'RESTAURACION', 'SALUD', 'TECNOLOGIA'])
  datos_importe = pd.read_csv('gs://bucket-prueba-nacho/datosSectorImporte.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  datos_grafica = datos_importe[datos_importe.SECTOR == sector]
  lista_franjas1 = []
  for i in range(len(datos_grafica.index)):
    lista_franjas1.append(i)

  importe = datos_grafica['sum(IMPORTE)']
  figura_imp = plt.bar(datos_grafica, x=lista_franjas1, y=importe)
  figura_imp.update_layout(title="<b><i>Importe TOTAL por Sector por Fanja Horaria</b></i>",xaxis_title="Franja Horaria", yaxis_title="Importe")
  figura_imp.update_traces(hovertemplate="Franja Horaria: %{x} <br> Importe: %{y}</br>")
  st.plotly_chart(figura_imp)

  datos_operaciones = pd.read_csv('gs://bucket-prueba-nacho/datosSectorOperaciones.csv', storage_options=({"token":"datos\grandes-volumenes-9d18b4ccbb2f.json"}), encoding="utf8")
  datos_grafica_2 = datos_operaciones[datos_importe.SECTOR == sector]
  lista_franjas2 = []
  for i in range(len(datos_grafica_2.index)):
    lista_franjas2.append(i)
  operaciones = datos_grafica_2['sum(NUM_OP)']
  figura_ops = plt.bar(datos_grafica_2, x=lista_franjas2, y=operaciones)
  figura_ops.update_layout(title="<b><i>Operaciones TOTALES por Sector por Fanja Horaria</b></i>",xaxis_title="Franja Horaria", yaxis_title="Operaciones")
  figura_ops.update_traces(hovertemplate="Franja Horaria: %{x} <br> Operaciones: %{y}</br>")
  st.plotly_chart(figura_ops)
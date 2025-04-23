import streamlit as st
import os
import sys
from PIL import Image
from datos.GestorDatos import GestorDatos


# Añadir la raíz del proyecto al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Clases internas
from visualizacion.Visualizador import Visualizador
# Calcula la ruta absoluta de main.py
BASE_DIR = os.path.dirname(__file__)
GD = GestorDatos()
visualizador = Visualizador()

GD.procesar_todo()
# Abre la imagen desde la misma carpeta que main.py
logo_path = os.path.join(BASE_DIR, "logo-cuc.png")
logo = Image.open(logo_path)


st.set_page_config(page_title="Informe Ejecutivo", layout="wide", page_icon="📊")
# Cachear modelo para no reentrenar cada vez

# Menú lateral
st.sidebar.image(logo, width=120)
#st.sidebar.title("Menú")
opcion = st.sidebar.radio("Seleccione una opción", ["KPIs", "Formulario de Predicción","Analisis EDA","Mapas Interactivos"])

if opcion == "KPIs":
    visualizador.carga_inicio()
elif opcion == "Formulario de Predicción":
    visualizador.carga_prediccion()

elif opcion == "Analisis EDA":
    st.title("Análisis EDA")
    submenu = st.sidebar.selectbox("Seleccione un análisis", ["Personas en Accidentes", "Accidentes con Víctimas"])

    if submenu == "Personas en Accidentes":
        visualizador.carga_eda_personas(submenu)

    elif submenu == "Accidentes con Víctimas":
        visualizador.carga_eda_accidentes(submenu)
elif opcion == "Mapas Interactivos":
    st.title("Mapas Interactivos")
    submenu = st.sidebar.selectbox("Seleccione un mapa", ["Personas en Accidentes", "Accidentes con Víctimas"])

    if submenu == "Personas en Accidentes":
       visualizador.carga_mapa_personas()

    elif submenu == "Accidentes con Víctimas":
        visualizador.carga_mapa_accidentes()


import streamlit as st
import os
import sys
from PIL import Image
from datos.GestorDatos import GestorDatos
from streamlit_modal import Modal


# Añadir la raíz del proyecto al path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Clases internas
from visualizacion.Visualizador import Visualizador
# Calcula la ruta absoluta de main.py
BASE_DIR = os.path.dirname(__file__)
GD = GestorDatos()
visualizador = Visualizador()

#GD.procesar_todo()
# Abre la imagen desde la misma carpeta que main.py
logo_path = os.path.join(BASE_DIR, "logo-cuc.png")
logo = Image.open(logo_path)


st.set_page_config(page_title="Informe Ejecutivo", layout="wide", page_icon="📊")
# Cachear modelo para no reentrenar cada vez

# Menú lateral
st.sidebar.image(logo, width=120)
#st.sidebar.title("Menú")
opcion = st.sidebar.radio("Seleccione una opción", ["Formulario de Predicción","Mapas Interactivos"])

if opcion == "Formulario de Predicción":
    visualizador.carga_prediccion()

elif opcion == "Mapas Interactivos":
    st.title("Mapas Interactivos")
    submenu = st.sidebar.selectbox("Seleccione un mapa", ["Personas en Accidentes", "Accidentes con Víctimas"])

    if submenu == "Personas en Accidentes":
       visualizador.carga_mapa_personas()

    elif submenu == "Accidentes con Víctimas":
        visualizador.carga_mapa_accidentes()

# Crear instancia del modal
modal = Modal("ℹ️ Acerca de esta Aplicación", key="acerca_modal", max_width=700)

# Botón en el menú lateral
with st.sidebar:
    if st.button("ℹ️ Acerca de"):
        modal.open()

# Mostrar el modal si está abierto
if modal.is_open():
    with modal.container():
        st.markdown("""
        Esta aplicación es una demostración del trabajo realizado por estudiantes del curso **BD-143 Programación II** del  
        **Diplomado en Big Data** del **Colegio Universitario de Cartago (CUC)** durante el **I Cuatrimestre 2025**,  
        como parte de su aprendizaje y desarrollo de habilidades en el campo del análisis de datos a gran escala.
        """)

        st.markdown("### 👩‍💻 Autores:")
        st.markdown("""
        - Nubia Brenes Valerín  
        - Pablo Marín Castillo  
        - Eduardo Núñez Morales
        """)

        st.markdown("### 🛠️ Herramientas y Tecnologías Utilizadas:")
        st.markdown("""
        - **Lenguaje de Programación:** Python  
        - **Librerías de Análisis de Datos:** Pandas, NumPy  
        - **Visualización de Datos:** Matplotlib, Seaborn, Plotly  
        - **Framework/Entorno:** Streamlit  
        - **Base de Datos:** PostgreSQL  
        - **Otras Herramientas:** Jupyter Notebooks, Microsoft Azure
        """)

st.sidebar.markdown(
    """
    <hr style='margin-top: 50px;'>
    <div style='font-size: 0.8em; text-align: center;'>
        © 2025 | Proyecto Final Programación II<br>
        Big Data CUC
    </div>
    """,
    unsafe_allow_html=True
)

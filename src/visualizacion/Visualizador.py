import streamlit as st
import pydeck as pdk



from src.basedatos.GestorBaseDatos import GestorBaseDatos
from src.modelos.ModeloML import ModeloML
from src.eda.ProcesadorEDA import ProcesadorEDA
from src.eda.AnalisisAccidentes import AnalisisAccidentes


class Visualizador:
    def __init__(self):
        self.BD = GestorBaseDatos()
        self.ML = ModeloML()



    def _cargar_modelo(_self):
        return _self.ML.procesar_modelo_ml()

    def carga_inicio(self):
        #col1, col2, col3 = st.columns(3)
        #col1.metric("Accidentes (últ. mes)", 120, "-5%")
        #col2.metric("Tasa (% población)", 0.03, "+0.2pp")
        #col3.metric("Tiempo resp. medio", "14 min", "-1 min")
        #st.markdown("---")
        #st.subheader("Tendencia de Accidentes por Provincia")
        # Aquí añadirías un gráfico Plotly con st.plotly_chart(...)
        #st.write("En el último trimestre, la provincia X acumuló un 20 % más de incidentes que…")

        # --- Obtener datos de accidentes ---
        df_victimas = self.BD.obtener_df_victimas()  # Aquí suponemos que obtienes los datos con las columnas correctas
        # Los datos deben tener 'anno' y 'mes_anno' para crear la fecha

        anal_v =  AnalisisAccidentes(df_victimas)

        # Calcular KPIs
        kpis_v = anal_v.calcular_kpis()

        # Mostrar KPIs en Streamlit
        col1, col2, col3 = st.columns(3)
        col1.metric("Accidentes Vict. (últ. mes)", f"{kpis_v['acc_ult']:,}", f"{kpis_v['pct_acc']:.1f}%")
        col2.metric("Tasa Vict. (% población)", f"{kpis_v['tasa_ult']:.3f}", f"{kpis_v['delta_tasa']:.3f} pp")

        st.plotly_chart(anal_v.tendencia_mensual(), use_container_width=True,
                        key=f"tendencia_mensual_victimas_{anal_v.df['fecha'].dt.year.min()}_{anal_v.df['fecha'].dt.month.min()}")
        # Para mostrar resultados en otro dataset (si existe otro dataset similar):
        df_personas = self.BD.obtener_df_personas()  # Reemplaza esto con el acceso correcto
        anal_p = AnalisisAccidentes(df_personas)
        kpis_p = anal_p.calcular_kpis()

        col1, col2, col3 = st.columns(3)
        col1.metric("Accidentes Pers. (últ. mes)", f"{kpis_p['acc_ult']:,}", f"{kpis_p['pct_acc']:.1f}%")
        col2.metric("Tasa Pers. (% población)", f"{kpis_p['tasa_ult']:.3f}", f"{kpis_p['delta_tasa']:.3f} pp")
        st.plotly_chart(anal_p.tendencia_mensual(), use_container_width=True, key=f"tendencia_mensual_personas_{anal_p.df['fecha'].dt.year.min()}_{anal_p.df['fecha'].dt.month.min()}")

    def carga_prediccion(self):
        st.title("Formulario para Predecir Accidentes")
        st.write("Complete los campos para obtener una predicción.")

        # Campos del formulario
        provincia = st.selectbox("Provincia", self.BD.obtener_list_provincias())
        if provincia:
            canton = st.selectbox("Cantón", self.BD.obtener_list_cantones(provincia))
        else:
            canton = st.selectbox("Cantón", ["Seleccione una provincia primero"], disabled=True)

        if provincia and canton:
            distrito = st.selectbox("Distrito", self.BD.obtener_list_distritos(provincia, canton))
        else:
            distrito = st.selectbox("Distrito", ["Seleccione un cantón primero"], disabled=True)

        tipo_ruta = st.selectbox("Tipo de Ruta", ["Nacional", "Cantonal"])
        dia_semana = st.selectbox("Día de la Semana",
                                  ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"])
        lluvia_acumulada = st.number_input("Lluvia Acumulada (mm)", min_value=0.0, step=0.1, max_value=99.9)
        hora = st.time_input("Hora del Evento")

        if st.button("Predecir"):
            datos = {
                'provincia': provincia,
                'canton': canton,
                'distrito': distrito,
                'tipo_ruta': tipo_ruta,
                'dia_semana': dia_semana,
                'lluvia_acumulada': lluvia_acumulada,
                'hora': hora.strftime('%H:%M:%S')
            }
            # Cargar modelo y log del entrenamiento
            modelo, log = self._cargar_modelo()

            # Realizar predicción
            resultado = self.ML.predecir_nuevo(modelo, **datos)

            st.success("✅ Predicción realizada con éxito")

            with st.expander("Ver resultados de la predicción", expanded=True):
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.metric("Predicción", "🚨 Accidente" if resultado['prediccion'] == 'SI' else "✅ No Accidente")
                with col2:
                    st.write("**Probabilidades por clase:**")
                    st.json(resultado['probabilidad'])

            with st.expander("🔍 Detalles del entrenamiento y validación"):
                st.code(log)

            st.toast(f"Resultado: {'Accidente' if resultado['prediccion'] == 1 else 'No Accidente'}", icon="📊")

    def carga_eda_personas(self, dataset_opcion):
        df = self.BD.obtener_df_personas()
        eda = ProcesadorEDA(df)

        # ℹ️ Información general
        with st.expander("ℹ️ Información General del Dataset"):
            st.text(eda.obtener_info_general())

        # 📈 Estadísticas Descriptivas
        with st.expander("📈 Estadísticas Descriptivas"):
            st.plotly_chart(eda.obtener_estadisticas_descriptivas(), use_container_width=True)

        # 🚫 Valores Faltantes
        with st.expander("🚫 Valores Faltantes"):
            st.dataframe(eda.detectar_valores_faltantes(return_df=True), use_container_width=True)

        # 📄 Registros Duplicados
        with st.expander("📄 Registros Duplicados"):
            duplicados_info = eda.detectar_valores_duplicados()
            st.write(f"Número de registros duplicados: {duplicados_info['numero_duplicados']}")
            if duplicados_info['primeras_filas'] is not None:
                st.dataframe(duplicados_info['primeras_filas'])

        # 📊 Distribuciones Numéricas
        with st.expander("📊 Distribuciones Numéricas"):
            distribuciones = eda.visualizar_distribuciones_numericas()
            for fig in distribuciones:
                st.plotly_chart(fig, use_container_width=True)

        # 📦 Distribuciones Categóricas
        with st.expander("📦 Distribuciones Categóricas"):
            columnas_cat = eda.df.select_dtypes(include='object').columns.tolist()

            seleccionadas = st.multiselect(
                "Selecciona columnas categóricas",
                columnas_cat,
                default=columnas_cat[:3],
                key=f"cols_cat_{dataset_opcion}"
            )

            # Filtro por columna
            columna_filtro = st.selectbox("¿Deseas aplicar un filtro por columna?", ["Ninguno"] + columnas_cat,
                                          key=f"filtro_col_{dataset_opcion}")
            valor_filtro = None
            if columna_filtro != "Ninguno":
                valores_unicos = eda.df[columna_filtro].dropna().unique().tolist()
                valor_filtro = st.selectbox(f"Selecciona un valor para filtrar por '{columna_filtro}'", valores_unicos,
                                            key=f"filtro_val_{dataset_opcion}")

            # Comparativa por año
            comparar_por_anno = False
            annos_disponibles = None
            if "anno" in eda.df.columns:
                comparar_por_anno = st.checkbox("¿Comparar por año?", key=f"comp_anno_{dataset_opcion}")
                if comparar_por_anno:
                    annos_disponibles = sorted(eda.df["anno"].dropna().unique())
                    annos_seleccionados = st.multiselect("Selecciona los años a comparar", annos_disponibles,
                                                         default=annos_disponibles[:2],
                                                         key=f"annos_sel_{dataset_opcion}")
                else:
                    annos_seleccionados = None
            else:
                st.warning("⚠️ No se encontró la columna 'anno'. Comparativa por año no disponible.")
                annos_seleccionados = None

            # Graficar distribuciones categóricas
            graficos_cat = eda.visualizar_distribuciones_categoricas(
                columnas=seleccionadas,
                columna_filtro=columna_filtro if columna_filtro != "Ninguno" else None,
                valor_filtro=valor_filtro,
                comparar_por_anno=comparar_por_anno,
                annos=annos_seleccionados
            )

            for col, fig in graficos_cat.items():
                if isinstance(fig, dict) and 'error' in fig:
                    st.warning(fig['error'])
                elif isinstance(fig, str):
                    st.warning(fig)
                else:
                    st.subheader(col)
                    st.plotly_chart(fig, use_container_width=True)

        # 🔗 Matriz de Correlación
        with st.expander("🔗 Matriz de Correlación"):
            resultado_corr = eda.visualizar_correlacion()
            if 'correlacion' in resultado_corr:
                st.plotly_chart(resultado_corr['correlacion'], use_container_width=True)
            else:
                st.warning(resultado_corr['error'])

    def carga_eda_accidentes(self,dataset_opcion):
        df = self.BD.obtener_df_victimas()
        eda = ProcesadorEDA(df)

        # ℹ️ Información general
        with st.expander("ℹ️ Información General del Dataset"):
            st.text(eda.obtener_info_general())

        # 📈 Estadísticas Descriptivas
        with st.expander("📈 Estadísticas Descriptivas"):
            st.plotly_chart(eda.obtener_estadisticas_descriptivas(), use_container_width=True)

        # 🚫 Valores Faltantes
        with st.expander("🚫 Valores Faltantes"):
            st.dataframe(eda.detectar_valores_faltantes(return_df=True), use_container_width=True)

        # 📄 Registros Duplicados
        with st.expander("📄 Registros Duplicados"):
            duplicados_info = eda.detectar_valores_duplicados()
            st.write(f"Número de registros duplicados: {duplicados_info['numero_duplicados']}")
            if duplicados_info['primeras_filas'] is not None:
                st.dataframe(duplicados_info['primeras_filas'])

        # 📊 Distribuciones Numéricas
        with st.expander("📊 Distribuciones Numéricas"):
            distribuciones = eda.visualizar_distribuciones_numericas()
            for fig in distribuciones:
                st.plotly_chart(fig, use_container_width=True)

        # 📦 Distribuciones Categóricas
        with st.expander("📦 Distribuciones Categóricas"):
            columnas_cat = eda.df.select_dtypes(include='object').columns.tolist()

            seleccionadas = st.multiselect(
                "Selecciona columnas categóricas",
                columnas_cat,
                default=columnas_cat[:3],
                key=f"cols_cat_{dataset_opcion}"
            )

            # Filtro por columna
            columna_filtro = st.selectbox("¿Deseas aplicar un filtro por columna?", ["Ninguno"] + columnas_cat,
                                          key=f"filtro_col_{dataset_opcion}")
            valor_filtro = None
            if columna_filtro != "Ninguno":
                valores_unicos = eda.df[columna_filtro].dropna().unique().tolist()
                valor_filtro = st.selectbox(f"Selecciona un valor para filtrar por '{columna_filtro}'", valores_unicos,
                                            key=f"filtro_val_{dataset_opcion}")

            # Comparativa por año
            comparar_por_anno = False
            annos_disponibles = None
            if "anno" in eda.df.columns:
                comparar_por_anno = st.checkbox("¿Comparar por año?", key=f"comp_anno_{dataset_opcion}")
                if comparar_por_anno:
                    annos_disponibles = sorted(eda.df["anno"].dropna().unique())
                    annos_seleccionados = st.multiselect("Selecciona los años a comparar", annos_disponibles,
                                                         default=annos_disponibles[:2],
                                                         key=f"annos_sel_{dataset_opcion}")
                else:
                    annos_seleccionados = None
            else:
                st.warning("⚠️ No se encontró la columna 'anno'. Comparativa por año no disponible.")
                annos_seleccionados = None

            # Graficar distribuciones categóricas
            graficos_cat = eda.visualizar_distribuciones_categoricas(
                columnas=seleccionadas,
                columna_filtro=columna_filtro if columna_filtro != "Ninguno" else None,
                valor_filtro=valor_filtro,
                comparar_por_anno=comparar_por_anno,
                annos=annos_seleccionados
            )

            for col, fig in graficos_cat.items():
                if isinstance(fig, dict) and 'error' in fig:
                    st.warning(fig['error'])
                elif isinstance(fig, str):
                    st.warning(fig)
                else:
                    st.subheader(col)
                    st.plotly_chart(fig, use_container_width=True)

        # 🔗 Matriz de Correlación
        with st.expander("🔗 Matriz de Correlación"):
            resultado_corr = eda.visualizar_correlacion()
            if 'correlacion' in resultado_corr:
                st.plotly_chart(resultado_corr['correlacion'], use_container_width=True)
            else:
                st.warning(resultado_corr['error'])

    def carga_mapa_accidentes(self):
        st.title("📍 Mapa Interactivo de Accidentes")

        try:
            df = self.BD.obtener_df_map_victimas()

            if df.empty:
                st.warning("El conjunto de datos está vacío.")
                return

            annos = df['anno'].dropna().sort_values().unique()
            tipos_accidente = df['tipo_accidente'].dropna().unique()
            franjas_horarias = df['franja_horaria_inicio'].dropna().unique()

            col1, col2, col3 = st.columns(3)
            with col1:
                anno_seleccionado = st.selectbox("Seleccione el año", annos)
            with col2:
                tipo_seleccionado = st.selectbox("Tipo de accidente", ["Todos"] + list(tipos_accidente))
            with col3:
                franja_seleccionada = st.selectbox("Franja horaria", ["Todas"] + list(franjas_horarias))

            df_filtrado = df[df['anno'] == anno_seleccionado]
            if tipo_seleccionado != "Todos":
                df_filtrado = df_filtrado[df_filtrado['tipo_accidente'] == tipo_seleccionado]
            if franja_seleccionada != "Todas":
                df_filtrado = df_filtrado[df_filtrado['franja_horaria_inicio'] == franja_seleccionada]

            if df_filtrado.empty:
                st.warning("No hay datos disponibles para los filtros seleccionados.")
                return

            if df_filtrado[['latitud', 'longitud']].isnull().any().any():
                st.error("Hay datos con coordenadas faltantes. No se puede generar el mapa.")
                return

            columnas_necesarias = [
                "longitud", "latitud", "clase_accidente", "tipo_accidente",
                "dia_semana", "mes_anno", "anno", "ruta", "kilometro"
            ]
            df_clean = df_filtrado[columnas_necesarias].dropna(subset=["longitud", "latitud"])
            df_clean = df_clean.astype({"longitud": float, "latitud": float})

            # Asignar colores por tipo de accidente
            tipo_accidentes = df_clean['tipo_accidente'].unique()
            colores = [
                [255, 0, 0],  # Rojo
                [0, 128, 255],  # Azul
                [0, 200, 100],  # Verde
                [255, 165, 0],  # Naranja
                [128, 0, 128],  # Morado
                [255, 255, 0],  # Amarillo
                [0, 255, 255],  # Celeste
                [255, 105, 180],  # Rosado
            ]

            color_dict = {tipo: colores[i % len(colores)] + [160] for i, tipo in enumerate(tipo_accidentes)}  # RGBA
            df_clean["color"] = df_clean["tipo_accidente"].map(color_dict)

            tooltip = {
                "html": "<b>Clase:</b> {clase_accidente}<br/>"
                        "<b>Tipo:</b> {tipo_accidente}<br/>"
                        "<b>Fecha:</b> {dia_semana}, {mes_anno} - {anno}<br/>"
                        "<b>Ruta:</b> {ruta} km {kilometro}",
                "style": {"backgroundColor": "black", "color": "white"}
            }

            view_state = pdk.ViewState(
                latitude=df_clean['latitud'].mean(),
                longitude=df_clean['longitud'].mean(),
                zoom=7
            )

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=df_clean.to_dict("records"),
                get_position='[longitud, latitud]',
                get_fill_color='color',
                get_radius=200,
                pickable=True,
                auto_highlight=True
            )

            st.pydeck_chart(pdk.Deck(
                map_style='mapbox://styles/mapbox/light-v9',
                initial_view_state=view_state,
                layers=[layer],
                tooltip=tooltip
            ), height=700)

            # Leyenda de colores
            st.markdown("### 🟢 Leyenda de tipos de accidente:")
            legend_html = "<div style='display: flex; flex-wrap: wrap;'>"
            for tipo, color in color_dict.items():
                rgba_str = f"rgba({color[0]}, {color[1]}, {color[2]}, {color[3] / 255})"
                legend_html += f"<div style='margin-right: 20px; margin-bottom: 10px;'>"
                legend_html += f"<span style='display: inline-block; width: 15px; height: 15px; background-color: {rgba_str}; border-radius: 50%; margin-right: 5px;'></span>{tipo}</div>"
            legend_html += "</div>"
            st.markdown(legend_html, unsafe_allow_html=True)

        except Exception as e:
            st.error(f"❌ Ocurrió un error al cargar el mapa: {e}")

    def carga_mapa_personas(self):
        st.title("📍 Mapa de Personas Accidentadas")

        try:
            df = self.BD.obtener_df_map_personas()  # Método que cargue tu dataset

            if df.empty:
                st.warning("El conjunto de datos está vacío.")
                return

            # Filtros
            annos = df['anno'].dropna().sort_values().unique()
            tipos_lesion = df['tipo_lesion'].dropna().unique()
            sexos = df['sexo'].dropna().unique()

            col1, col2, col3 = st.columns(3)
            with col1:
                anno_seleccionado = st.selectbox("Seleccione el año", annos)
            with col2:
                tipo_lesion_sel = st.selectbox("Tipo de lesión", ["Todos"] + list(tipos_lesion))
            with col3:
                sexo_sel = st.selectbox("Sexo", ["Todos"] + list(sexos))

            df_filtrado = df[df['anno'] == anno_seleccionado]
            if tipo_lesion_sel != "Todos":
                df_filtrado = df_filtrado[df_filtrado['tipo_lesion'] == tipo_lesion_sel]
            if sexo_sel != "Todos":
                df_filtrado = df_filtrado[df_filtrado['sexo'] == sexo_sel]

            if df_filtrado.empty:
                st.warning("No hay datos disponibles para los filtros seleccionados.")
                return

            if df_filtrado[['latitud', 'longitud']].isnull().any().any():
                st.error("Hay datos con coordenadas faltantes.")
                return

            columnas_necesarias = [
                "longitud", "latitud", "rol", "tipo_lesion", "edad", "sexo", "vehiculo",
                "dia_semana", "mes_anno", "anno"
            ]
            df_clean = df_filtrado[columnas_necesarias].dropna(subset=["latitud", "longitud"])
            df_clean = df_clean.astype({"longitud": float, "latitud": float})

            # Colores por tipo_lesion
            tipos = df_clean['tipo_lesion'].unique()
            colores = [
                [255, 0, 0], [0, 128, 255], [0, 200, 100], [255, 165, 0],
                [128, 0, 128], [255, 255, 0], [0, 255, 255], [255, 105, 180]
            ]
            color_dict = {tipo: colores[i % len(colores)] + [160] for i, tipo in enumerate(tipos)}
            df_clean["color"] = df_clean["tipo_lesion"].map(color_dict)

            tooltip = {
                "html": "<b>Rol:</b> {rol}<br/>"
                        "<b>Lesión:</b> {tipo_lesion}<br/>"
                        "<b>Edad:</b> {edad}<br/>"
                        "<b>Sexo:</b> {sexo}<br/>"
                        "<b>Vehículo:</b> {vehiculo}<br/>"
                        "<b>Fecha:</b> {dia_semana}, {mes_anno} - {anno}",
                "style": {"backgroundColor": "black", "color": "white"}
            }

            view_state = pdk.ViewState(
                latitude=df_clean['latitud'].mean(),
                longitude=df_clean['longitud'].mean(),
                zoom=7
            )

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=df_clean.to_dict("records"),
                get_position='[longitud, latitud]',
                get_fill_color='color',
                get_radius=200,
                pickable=True,
                auto_highlight=True
            )

            st.pydeck_chart(pdk.Deck(
                map_style='mapbox://styles/mapbox/light-v9',
                initial_view_state=view_state,
                layers=[layer],
                tooltip=tooltip
            ), height=700)

            # Leyenda de colores
            st.markdown("### 🟢 Leyenda de tipos de lesión:")
            legend_html = "<div style='display: flex; flex-wrap: wrap;'>"
            for tipo, color in color_dict.items():
                rgba_str = f"rgba({color[0]}, {color[1]}, {color[2]}, {color[3] / 255})"
                legend_html += f"<div style='margin-right: 20px; margin-bottom: 10px;'>"
                legend_html += f"<span style='display: inline-block; width: 15px; height: 15px; background-color: {rgba_str}; border-radius: 50%; margin-right: 5px;'></span>{tipo}</div>"
            legend_html += "</div>"
            st.markdown(legend_html, unsafe_allow_html=True)

        except Exception as e:
            st.error(f"❌ Error al cargar el mapa de personas: {e}")
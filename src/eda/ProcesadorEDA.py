from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import io
import os
plt.style.use('ggplot')

class ProcesadorEDA:
    def __init__(self, df):
        self.df = df

    @classmethod
    def carga_accidentes_victimas(cls):
        BASE_DIR = Path(__file__).resolve().parents[2]
        csv_path = BASE_DIR / "data" / "processed" / "accidentes_victimas_tb.csv"

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Archivo no encontrado en: {csv_path}")

        df = pd.read_csv(csv_path, sep=';', encoding='utf-8')
        df.columns = df.columns.str.strip()
        return cls(df)

    @classmethod
    def carga_base_personas(cls):
        BASE_DIR = Path(__file__).resolve().parents[2]
        csv_path = BASE_DIR / "data" / "processed" / "base_personas_accidentes_tb.csv"

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Archivo no encontrado en: {csv_path}")

        df = pd.read_csv(csv_path, sep=';', encoding='utf-8')
        df.columns = df.columns.str.strip()
        return cls(df)

    def obtener_info_general(self):
        try:
            buffer = io.StringIO()
            self.df.info(buf=buffer)
            info_text = buffer.getvalue()
            return info_text
        except Exception as e:
            return f"Error al obtener la información general: {e}"

    def obtener_estadisticas_descriptivas(self):
        try:
            estadisticas = self.df.describe().round(2)
            header_values = ["Estadística"] + list(estadisticas.columns)
            cell_values = [[index] + list(estadisticas.loc[index]) for index in estadisticas.index]
            cell_values_transposed = list(map(list, zip(*cell_values)))

            fig = go.Figure(data=[go.Table(
                columnwidth=[100] + [80] * len(estadisticas.columns),
                header=dict(values=header_values, fill_color='lightblue', align='left'),
                cells=dict(values=cell_values_transposed, fill_color='white', align='left')
            )])
            fig.update_layout(title="Estadísticas Descriptivas de Columnas Numéricas")
            return fig
        except Exception as e:
            return f"Error al obtener estadísticas descriptivas: {e}\n"

    def mostrar_valores_unicos(self, columnas=None):
        try:
            resultado = {}
            columnas_categoricas = columnas if columnas else self.df.select_dtypes(include='object').columns
            for col in columnas_categoricas:
                if col in self.df.columns:
                    resultado[col] = self.df[col].unique().tolist()
                else:
                    resultado[col] = "Columna no encontrada."
            return resultado
        except Exception as e:
            return {"error": f"Error al mostrar los valores únicos: {e}"}

    def visualizar_distribuciones_numericas(self, columnas=None, bins=10):
        try:
            figs = []
            if columnas is None:
                columnas = self.df.select_dtypes(include=['int64', 'float64']).columns
            for col in columnas:
                if col in self.df.columns and self.df[col].dtype in ['int64', 'float64']:
                    fig = px.histogram(self.df, x=col, nbins=bins, marginal="rug", title=f"Distribución de {col}")
                    fig.update_layout(xaxis_title=col, yaxis_title="Frecuencia")
                    figs.append(fig)
            return figs
        except Exception as e:
            return f"Error: {e}"

    def visualizar_distribuciones_categoricas(self, columnas=None, columna_filtro=None, valor_filtro=None,
                                              comparar_por_anno=False, annos=None):
        """
        Genera gráficos de distribución categórica y los devuelve en un diccionario.

        Returns:
            dict: Un diccionario con los nombres de las columnas como claves y los objetos `fig` de Plotly como valores.
        """
        figuras = {}
        try:
            # Verifica si 'anno' existe para permitir el comparativo
            if comparar_por_anno and 'anno' not in self.df.columns:
                figuras['error'] = "No se encontró la columna 'anno'. No se puede realizar el comparativo por año."
                return figuras

            # Filtro base
            df_filtrado = self.df.copy()

            # Filtro por columna
            if columna_filtro and valor_filtro is not None:
                if columna_filtro in df_filtrado.columns:
                    df_filtrado = df_filtrado[df_filtrado[columna_filtro] == valor_filtro]
                else:
                    figuras['warning'] = f"La columna de filtro '{columna_filtro}' no existe. Se ignorará el filtro."

            # Filtro por años específicos
            if comparar_por_anno and annos:
                df_filtrado = df_filtrado[df_filtrado['anno'].isin(annos)]

            # Columnas a graficar
            if columnas is None:
                columnas_categoricas = df_filtrado.select_dtypes(include='object').columns
            else:
                columnas_categoricas = columnas

            for col in columnas_categoricas:
                if col not in df_filtrado.columns:
                    figuras[col] = f"La columna '{col}' no existe en el DataFrame."
                    continue
                if df_filtrado[col].dtype != 'object':
                    figuras[col] = f"La columna '{col}' no es categórica."
                    continue

                # Gráfico con o sin comparativo por año
                if comparar_por_anno:
                    fig = px.histogram(
                        df_filtrado,
                        x=col,
                        color='anno',
                        barmode='group',
                        category_orders={col: sorted(df_filtrado[col].dropna().unique())},
                        color_discrete_sequence=px.colors.qualitative.Set1
                    )
                    fig.update_layout(
                        title=f"Comparativo de {col} por Año" +
                              (f" (filtrado por {columna_filtro} = {valor_filtro})" if columna_filtro else ''),
                        xaxis_title=col,
                        yaxis_title="Conteo",
                        bargap=0.2
                    )
                    fig.update_traces(hovertemplate='Año: %{marker.color}<br>Categoría: %{x}<br>Conteo: %{y}')
                else:
                    fig = px.histogram(
                        df_filtrado,
                        x=col,
                        color_discrete_sequence=['#636EFA']
                    )
                    fig.update_layout(
                        title=f"Distribución de {col}" +
                              (f" (filtrado por {columna_filtro} = {valor_filtro})" if columna_filtro else ''),
                        xaxis_title=col,
                        yaxis_title="Conteo",
                        bargap=0.2
                    )
                    fig.update_traces(hovertemplate='Categoría: %{x}<br>Conteo: %{y}')

                figuras[col] = fig

            return figuras

        except Exception as e:
            figuras['error'] = f"Error al visualizar distribuciones categóricas: {e}"
            return figuras

    def detectar_valores_faltantes(self, return_df=False):
        try:
            nulos = self.df.isnull().sum()
            porcentaje = (nulos / len(self.df)) * 100
            tabla = pd.DataFrame({'Nulos': nulos, 'Porcentaje': porcentaje})
            tabla = tabla[tabla['Nulos'] > 0].sort_values(by='Nulos', ascending=False)
            return tabla if return_df else tabla.to_string()
        except Exception as e:
            return f"Error: {e}"

    def detectar_valores_duplicados(self):
        try:
            duplicados = self.df.duplicated().sum()
            primeras_filas = self.df[self.df.duplicated(keep='first')].head() if duplicados > 0 else None
            return {
                "numero_duplicados": duplicados,
                "primeras_filas": primeras_filas
            }
        except Exception as e:
            return {"error": f"Error al detectar duplicados: {e}"}

    def visualizar_correlacion(self, metodo='pearson'):
        """
        Genera la matriz de correlación entre variables numéricas.

        Returns:
            dict: {'correlacion': fig} si todo sale bien, o {'error': mensaje} si hay fallo.
        """
        resultado = {}
        try:
            columnas_numericas = self.df.select_dtypes(include=['int64', 'float64']).columns
            if len(columnas_numericas) <= 1:
                resultado['error'] = "No hay suficientes columnas numéricas para calcular la correlación."
                return resultado

            matriz_correlacion = self.df[columnas_numericas].corr(method=metodo)

            fig = px.imshow(
                matriz_correlacion,
                text_auto=".2f",
                color_continuous_scale='RdBu',
                zmin=-1, zmax=1,
                title=f"Matriz de Correlación ({metodo})"
            )
            fig.update_layout(xaxis_title="Variables", yaxis_title="Variables")
            resultado['correlacion'] = fig
            return resultado

        except Exception as e:
            resultado['error'] = f"Error al visualizar la correlación: {e}"
            return resultado

    def realizar_eda(self):  # Combo para ejecutar todos los métodos del EDA en secuencia
        try:
            return {
                "info_general": self.obtener_info_general(),
                "estadisticas": self.obtener_estadisticas_descriptivas(),
                "valores_unicos": self.mostrar_valores_unicos(),
                "graficos_numericos": self.visualizar_distribuciones_numericas(),
                "graficos_categoricos": self.visualizar_distribuciones_categoricas(),
                "faltantes": self.detectar_valores_faltantes(return_df=True),
                "duplicados": self.detectar_valores_duplicados(),
                "correlacion": self.visualizar_correlacion()}
        except Exception as e:
            print(f"Error al realizar el EDA: {e}\n")

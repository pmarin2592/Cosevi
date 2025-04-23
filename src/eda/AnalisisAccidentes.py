import pandas as pd
import plotly.express as px
from src.api.ClienteAPI import ClienteAPI


class AnalisisAccidentes:
    """
        Clase genérica para KPIs y tendencias mensuales,
        recibe un DataFrame con columna 'fecha' generada a partir de 'anno' y 'mes_anno'.
        """

    def __init__(self, df, inicio='2020-01-01', fin='2023-12-31'):
        # Generamos la columna 'fecha' a partir de 'anno' y 'mes_anno'

        self.df = df

        meses = {
            'Enero': '01', 'Febrero': '02', 'Marzo': '03', 'Abril': '04',
            'Mayo': '05', 'Junio': '06', 'Julio': '07', 'Agosto': '08',
            'Setiembre': '09', 'Octubre': '10', 'Noviembre': '11', 'Diciembre': '12'
        }
        self.df['mes_anno'] = self.df['mes_anno'].astype(str).str.strip().str.capitalize()

        # Reemplaza los nombres de meses en la columna
        self.df['mes_anno'] = self.df['mes_anno'].map(meses)

        # Filtrar datos inválidos antes de convertir
        self.df = self.df[self.df['mes_anno'].notna() & self.df['anno'].notna()]

        # Convertir usando formato explícito
        self.df['fecha'] = pd.to_datetime(
            self.df['anno'].astype(int).astype(str) + '-' + self.df['mes_anno'].astype(int).astype(str).str.zfill(
                2) + '-01',
            format='%Y-%m-%d',
            errors='coerce'  # Opcional: si algo falla, lo convierte en NaT en lugar de lanzar error
        )

        # Filtramos por el rango de fechas
        mask = (self.df['fecha'] >= pd.to_datetime(inicio)) & (self.df['fecha'] <= pd.to_datetime(fin))
        self.df = self.df.loc[mask]

        # Eliminar filas con fechas NaT
        self.df = self.df.dropna(subset=['fecha'])

        self.api = ClienteAPI()

        print(self.df)

    def calcular_kpis(self):
        """
              Calcula los KPIs basados en el último mes disponible en el dataset:
                - Accidentes último mes y variación vs mes anterior
                - Tasa % población y variación
              """
        if self.df.empty:
            print("Dataset vacío luego del preprocesamiento.")
            return {
                'acc_ult': 0,
                'pct_acc': 0,
                'tasa_ult': 0,
                'delta_tasa': 0,
            }

        # Obtener el último mes disponible en los datos
        ultimo_mes = self.df['fecha'].max().to_period('M').to_timestamp()
        mes_anterior = (ultimo_mes - pd.offsets.MonthBegin(1))

        print(f"Último mes disponible en los datos: {ultimo_mes.strftime('%Y-%m')}")
        print(f"Mes anterior: {mes_anterior.strftime('%Y-%m')}")

        df_ult = self.df[
            (self.df['fecha'] >= ultimo_mes) & (self.df['fecha'] < (ultimo_mes + pd.offsets.MonthBegin(1)))]
        df_prev = self.df[(self.df['fecha'] >= mes_anterior) & (self.df['fecha'] < ultimo_mes)]

        print("Accidentes último mes:", len(df_ult))
        print("Accidentes mes anterior:", len(df_prev))
        print("Fechas en el dataset:", self.df['fecha'].min(), "→", self.df['fecha'].max())

        acc_ult = len(df_ult)
        acc_prev = len(df_prev)
        pct_acc = (acc_ult - acc_prev) / acc_prev * 100 if acc_prev else 0

        # Población total
        poblacion = self.api.obtener_poblacion("CR", 2023)
        tasa_ult = acc_ult / poblacion * 100
        tasa_prev = acc_prev / poblacion * 100
        delta_tasa = tasa_ult - tasa_prev

        return {
            'acc_ult': acc_ult,
            'pct_acc': pct_acc,
            'tasa_ult': tasa_ult,
            'delta_tasa': delta_tasa,
        }

    def tendencia_mensual(self, provincia_field='provincia'):
        """
        Devuelve un gráfico Plotly con accidentes mensuales por provincia.
        """
        df2 = self.df
        df2['mes'] = df2.fecha.dt.to_period('M').dt.to_timestamp()

        trend = (
            df2
            .groupby(['mes', df2[provincia_field]])
            .size()
            .reset_index(name='accidentes')
            .rename(columns={provincia_field: 'provincia'})
        )

        # Verifica los datos agrupados
        print(f"Datos agrupados: {trend.head()}")  # Verifica los primeros registros

        # Si el DataFrame está vacío, muestra un mensaje
        if trend.empty:
            print("No hay datos para la tendencia mensual.")

        fig = px.line(
            trend,
            x='mes', y='accidentes',
            color='provincia',
            title='Accidentes mensuales por provincia'
        )

        return fig
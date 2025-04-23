#Importar librerias
from pathlib import Path
import os
import pandas as pd
from basedatos.GestorBaseDatos import GestorBaseDatos
from api.ClienteAPI import ClienteAPI

class GestorDatos:
    def __init__(self):
        self.df = None
        self.df_transformado = None
        self.BD = GestorBaseDatos()
        self.API = ClienteAPI()
    def procesar_todo(self):
        if self.BD.validar_data_cargada():
            self._procesar_accidentes_victimas()
            self._procesar_personas_accidentes()
            df=self.BD.carga_ubicaciones()
            self.API.cargar_lat_lon(df)

    def _procesar_accidentes_victimas(self):
        print("Procesando archivo de accidentes con víctimas...")
        ruta_entrada = self._ruta_accidentes_victimas()
        self._cargar_datos(ruta_entrada)
        self._transformar_accidentes()
        self.nombre_archivo_salida = "accidentes_victimas_tb.csv"
        self._guardar_datos()
        self.BD.carga_acidentes_victimnas()

    def _procesar_personas_accidentes(self):
        print("Procesando archivo de personas en accidentes...")
        ruta_entrada = self._ruta_personas_accidentes()
        self._cargar_datos(ruta_entrada)
        self._transformar_personas()
        self.nombre_archivo_salida = "base_personas_accidentes_tb.csv"
        self._guardar_datos()
        self.BD.carga_personas_accidentes()

    def _cargar_datos(self, ruta_csv):
        self.df = pd.read_csv(ruta_csv, delimiter=';')
        self.df.columns = self.df.columns.str.strip()
        self.df = self.df.where(pd.notnull(self.df), None)

    def _transformar_accidentes(self):
        df = self.df.copy()
        df.rename(columns={
            'Clase de accidente': 'clase_accidente',
            'Tipo de accidente': 'tipo_accidente',
            'Año': 'anno',
            'Hora': 'hora',
            'Provincia': 'provincia',
            'Cantón': 'canton',
            'Distrito': 'distrito',
            'Ruta': 'ruta',
            'Kilómetro': 'kilometro',
            'Rural o urbano': 'rural_urbano',
            'Calzada vertical': 'calzada_vertical',
            'Calzada horizontal': 'calzada_horizontal',
            'Tipo de calzada': 'tipo_calzada',
            'Tipo de circulación': 'tipo_circulacion',
            'Estado del tiempo': 'estado_tiempo',
            'Estado de la calzada': 'estado_calzada',
            'Región Mideplan': 'region_mideplan',
            'Tipo ruta': 'tipo_ruta',
            'Día': 'dia',
            'Mes': 'mes'
        }, inplace=True)

        df['franja_horaria_inicio'] = df['hora'].str[:5]
        df['franja_horaria_fin'] = df['hora'].str[-5:]
        df['dia_semana'] = df['dia'].str[2:]
        df['mes_anno'] = df['mes'].str[2:]

        columnas_finales = [
            'clase_accidente','tipo_accidente', 'dia_semana', 'mes_anno', 'anno',
            'franja_horaria_inicio', 'franja_horaria_fin',
            'provincia', 'canton', 'distrito', 'ruta', 'kilometro',
            'tipo_ruta', 'rural_urbano', 'region_mideplan',
            'calzada_vertical', 'calzada_horizontal', 'tipo_calzada',
            'estado_calzada', 'tipo_circulacion', 'estado_tiempo'
        ]

        self.df_transformado = df[columnas_finales]

    def _transformar_personas(self):
        df = self.df.copy()
        df.rename(columns={
            'Rol': 'rol',
            'Tipo de lesión': 'tipo_lesion',
            'Edad': 'edad',
            'Sexo': 'sexo',
            'Vehiculo en  el que viajaba': 'vehiculo',
            'Año': 'anno',
            'Provincia': 'provincia',
            'Cantón': 'canton',
            'Distrito': 'distrito',
            'Día': 'dia_semana',
            'Mes': 'mes_anno',
            'Edad quinquenal': 'edad_quinquenal'
        }, inplace=True)

        df['edad'] = df['edad'].replace('Desconocido', None).astype(float).astype('Int64')
        df['edad_quinquenal'] = df['edad_quinquenal'].replace('Desconocida', None)
        df['sexo'] = df['sexo'].replace('Desconocido', None)
        df['rol'] = df['rol'].replace('Desconocido', 'Otro')
        df['edad_quinquenal'] = df['edad_quinquenal'].str[2:].str.replace('a', '-', regex=False)
        df['dia_semana'] = df['dia_semana'].str[2:]
        df['mes_anno'] = df['mes_anno'].str[2:]

        columnas_finales = [
            'rol', 'tipo_lesion', 'edad', 'sexo', 'vehiculo',
            'dia_semana', 'mes_anno', 'anno',
            'provincia', 'canton', 'distrito', 'edad_quinquenal'
        ]

        df = df.dropna()

        self.df_transformado = df[columnas_finales]

    def _guardar_datos(self):
        if self.df_transformado is None:
            raise Exception("Primero debes transformar los datos.")
        if not self.nombre_archivo_salida:
            raise Exception("No se ha definido el nombre del archivo de salida.")

        anno_maximo = self.df_transformado['anno'].max()
        self.df_transformado = self.df_transformado[self.df_transformado['anno'] >= anno_maximo - 2]

        BASE_DIR = Path(__file__).resolve().parents[2]
        ruta_salida = BASE_DIR / "data" / "processed" / self.nombre_archivo_salida

        if ruta_salida.exists():
            os.remove(ruta_salida)

        self.df_transformado.to_csv(ruta_salida, index=False, encoding='utf-8-sig')
        print(f"✅ Archivo guardado: {ruta_salida}")

    def _ruta_accidentes_victimas(self):
        BASE_DIR = Path(__file__).resolve().parents[2]
        ruta = BASE_DIR / "data" / "raw" / "2 Base de accidentes con victimas 2017_ 2023_UTF8.csv"
        if not ruta.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {ruta}")
        return str(ruta)

    def _ruta_personas_accidentes(self):
        BASE_DIR = Path(__file__).resolve().parents[2]
        ruta = BASE_DIR / "data" / "raw" / "3 Base de personas en accidentes 2017_ 2023_UTF8.csv"
        if not ruta.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {ruta}")
        return str(ruta)



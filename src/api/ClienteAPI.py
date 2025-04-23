from datetime import datetime

import requests

from basedatos.GestorBaseDatos import GestorBaseDatos
from helpers.Utilidades import Utilidades
from tqdm import tqdm
from collections import defaultdict

class ClienteAPI:
    def __init__(self):
        self.bd = GestorBaseDatos()
        self.util = Utilidades()

    def _buscar_lugar_api(self,provincia, canton, distrito):
        try:
            # Construir la consulta con los campos separados
            query = f"{distrito}, {canton}, {provincia}, Costa Rica"

            # Parámetros para Nominatim
            params = {
                'q': query,
                'format': 'json',
                'addressdetails': 1,
                'limit': 1
            }
            headers = {
                "User-Agent": "Analisis/1.0"  # Pon tu email real si es posible
            }

            response = requests.get('https://nominatim.openstreetmap.org/search', params=params, headers=headers,
                                    timeout=10)

            # Comprobar si la respuesta fue exitosa
            response.raise_for_status()

            data = response.json()

            if not data:
                return {
                    'success': False,
                    'message': 'No se encontraron resultados para la ubicación proporcionada.'
                }

            lugar = data[0]
            return {
                'success': True,
                'nombre': lugar['display_name'],
                'latitud': lugar['lat'],
                'longitud': lugar['lon'],
                'detalles': lugar['address']
            }

        except requests.exceptions.Timeout:
            return {
                'success': False,
                'message': 'La solicitud a la API tardó demasiado tiempo y fue cancelada.'
            }
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'message': f'Error de red o solicitud: {str(e)}'
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Ocurrió un error inesperado: {str(e)}'
            }

    def _consultar_lluvia_api(self,lat, lon, start_date="2023-10-01", end_date="2023-12-31"):
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "precipitation",
            "timezone": "auto"
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        registros = []
        # Mapeo de días en inglés a español sin tildes
        dias_es = {
            "Monday": "Lunes",
            "Tuesday": "Martes",
            "Wednesday": "Miercoles",
            "Thursday": "Jueves",
            "Friday": "Viernes",
            "Saturday": "Sabado",
            "Sunday": "Domingo"
        }
        # Diccionario para agrupar: (anio, mes, dia_semana, hora) -> lista de precipitaciones
        lluvia_agrupada = defaultdict(list)
        # Agrupar precipitaciones por año, mes, día de la semana y hora
        for i, time in enumerate(data["hourly"]["time"]):
            dt = datetime.fromisoformat(time)
            anio = dt.year
            mes = dt.month
            dia_en = dt.strftime("%A")  # Ej: 'Monday'
            dia_semana = dias_es[dia_en]  # Convertido a español sin tilde
            hora = dt.strftime("%H:%M")
            precipitacion = data["hourly"]["precipitation"][i]

            clave = (anio, mes, dia_semana, hora)
            lluvia_agrupada[clave].append(precipitacion)

        for (anio, mes, dia_semana, hora), lista_lluvia in lluvia_agrupada.items():
            promedio = sum(lista_lluvia) / len(lista_lluvia)
            registros.append({
                "anio": anio,
                "mes": mes,
                "dia_semana": dia_semana,
                "hora": hora,
                "lluvia_acumulada": round(promedio, 2)
            })

        # Ordenar por día de la semana en español y hora
        dias_orden = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
        registros.sort(key=lambda x: (x["anio"], x["mes"], dias_orden.index(x["dia_semana"]), x["hora"]))

        return registros

    def cargar_lat_lon(self,df, chunk_size=1000):
        if not df.empty:
            print("Iniciando actualización de ubicaciones...")

            # Dividir el DataFrame en chunks
            chunks = list(self.util.dividir_en_chunks(df, chunk_size))

            # Barra de progreso para la actualización
            for chunk in tqdm(chunks, desc="Actualizando registros", ncols=100):
                for _, row in chunk.iterrows():
                    resultado = self._buscar_lugar_api(row['provincia'], row['canton'], row['distrito'])
                    print(resultado)
                    if resultado['success']:
                      self.bd.actualizar_ubicaciones(row['id'], resultado['longitud'], resultado['latitud'])
        else:
            print("⚠️ DataFrame vacío, no se puede procesar.")

    def carga_precipitacion(self,df, chunk_size=1000):
        if not df.empty:
            print("Iniciando insercion de precipitaciones...")

            # Dividir el DataFrame en chunks
            chunks = list(self.util.dividir_en_chunks(df, chunk_size))

            # Barra de progreso para la actualización
            for chunk in tqdm(chunks, desc="Insertando registros", ncols=100):
                for _, row in chunk.iterrows():
                    try:
                        provincia = row['provincia']
                        canton = row['canton']
                        distrito = row['distrito']
                        lat = row['latitud']
                        lon = row['longitud']

                        datos_lluvia = self._consultar_lluvia_api(lat, lon)
                        print(datos_lluvia)
                        for registro in datos_lluvia:
                            print(registro)
                            self.bd.insertar_lluvia(provincia, canton, distrito, registro['dia_semana'], registro['mes'],
                                            registro['anio'], registro['hora'], registro['lluvia_acumulada'])
                    except Exception as e:
                        print(f"⚠️ Error inesperado al insertar: {e}")
        else:
            print("⚠️ DataFrame vacío, no se puede procesar.")

    def obtener_poblacion(self,country_code: str, year: int) -> int:
        """
        Consulta la población total del country_code en el año indicado
        usando la API del Banco Mundial, con control de excepciones.
        """
        url = "http://api.worldbank.org/v2/country/{}/indicator/SP.POP.TOTL".format(country_code)
        params = {
            "date": year,
            "format": "json"
        }

        try:
            # Realizar la solicitud a la API
            resp = requests.get(url, params=params, timeout=10)

            # Verificar que la respuesta sea exitosa (código 200)
            resp.raise_for_status()

            # Procesar la respuesta JSON
            data = resp.json()

            # Verificar que los datos existan en la respuesta
            if len(data) < 2 or len(data[1]) == 0 or "value" not in data[1][0]:
                raise ValueError(f"No se encontró la población para el código de país {country_code} en el año {year}")

            # Obtener la población desde la respuesta
            poblacion = data[1][0].get("value")

            # Validar si la población es nula
            if poblacion is None:
                raise ValueError(f"No se encontró un valor de población para {country_code} en {year}")

            return int(poblacion)

        except requests.exceptions.RequestException as e:
            # Manejo de excepciones relacionadas con la solicitud HTTP
            print(f"Error de solicitud HTTP: {e}")
        except ValueError as e:
            # Manejo de excepciones relacionadas con el procesamiento de los datos
            print(f"Error en los datos obtenidos: {e}")
        except Exception as e:
            # Capturar otros errores generales
            print(f"Ocurrió un error inesperado: {e}")

        return None  # Retornar None si ocurre algún error

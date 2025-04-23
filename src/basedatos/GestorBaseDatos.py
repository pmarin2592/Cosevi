from pathlib import Path
import psycopg2
from configparser import ConfigParser
import os
import pandas as pd
from psycopg2.extras import execute_values
from tqdm import tqdm
from psycopg2.extras import execute_values
from helpers.Utilidades import Utilidades
import time

class GestorBaseDatos:
    def __init__(self):
        self.util = Utilidades()

    def _config(self,filename=os.path.abspath(os.path.join(os.path.dirname(__file__), '../config.ini')),
               section='postgresql'):
        parser = ConfigParser()
        parser.read(os.path.abspath(filename))

        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception(f'Sección {section} no encontrada en el archivo {filename}')

        return db

    def _conectar(self,max_reintentos=20, espera_segundos=3):
        conn = None
        for intento in range(1, max_reintentos + 1):
            try:
                params = self._config()
                conn = psycopg2.connect(**params)
                print("✅ Conexión exitosa a PostgreSQL")
                return conn
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"⚠️ Intento {intento}/{max_reintentos} fallido: {error}")
                if intento == max_reintentos:
                    print(
                        "❌ Error al conectar: connection to server at \"40.76.114.77\", port 5432 failed: Connection timed out (0x0000274C/10060)\n¿Está el servidor ejecutándose y aceptando conexiones TCP/IP?")
                    return None
                time.sleep(espera_segundos)  # Espera entre reintentos

    def carga_acidentes_victimnas(self):

        # Obtiene el directorio base del proyecto (2 niveles arriba desde este script)
        BASE_DIR = Path(__file__).resolve().parents[2]  # Subís 2 niveles
        csv_path = BASE_DIR / "data" / "processed" / "accidentes_victimas_tb.csv"

        # Comprobación opcional (te puede ayudar a depurar si vuelve a fallar)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Archivo no encontrado en: {csv_path}")

        df = pd.read_csv(csv_path, sep=',',encoding='utf-8')
        df.columns = df.columns.str.strip()
        df = df.where(pd.notnull(df), None)  # Convertir NaN a None
        print(df)

        conn = self._conectar()
        cursor = conn.cursor()

        query = """
        INSERT INTO dwh.accidentes_victimas_tb
        (clase_accidente, tipo_accidente, dia_semana, mes_anno, anno, franja_horaria_inicio, franja_horaria_fin, 
        provincia, canton, distrito, ruta, kilometro, tipo_ruta, rural_urbano, region_mideplan, calzada_vertical, 
        calzada_horizontal, tipo_calzada, estado_calzada, tipo_circulacion, estado_tiempo)
        VALUES %s
        """
        values = [tuple(row) for row in df.itertuples(index=False)]
        # Definir el tamaño de cada lote; puede ajustarse según tus necesidades
        chunk_size = 1000
        chunks = list( self.util.dividir_en_chunks(values, chunk_size))
        print("Iniciando inserción de datos...")
        # Barra de progreso: total de chunks
        for chunk in tqdm(chunks, desc="Insertando registros", ncols=100):
            execute_values(cursor, query, chunk)
            conn.commit()  # Commit por cada chunk

        print("Datos insertados exitosamente.")

        cursor.close()
        conn.close()

    def carga_personas_accidentes(self):
        # Directorio del archivo
        BASE_DIR = Path(__file__).resolve().parents[2]
        csv_path = BASE_DIR / "data" / "processed" / "base_personas_accidentes_tb.csv"

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Archivo no encontrado en: {csv_path}")

        df = pd.read_csv(csv_path, sep=',', encoding='utf-8')
        df.columns = df.columns.str.strip()

        # Validación de longitud y tipos
        df['rol'] = df['rol'].astype(str).str.strip().str[:25]
        df['tipo_lesion'] = df['tipo_lesion'].astype(str).str.strip().str[:16]
        df['sexo'] = df['sexo'].astype(str).str.strip().str[:12]
        df['vehiculo'] = df['vehiculo'].astype(str).str.strip().str[:22]
        df['provincia'] = df['provincia'].astype(str).str.strip().str[:10]
        df['canton'] = df['canton'].astype(str).str.strip().str[:20]
        df['distrito'] = df['distrito'].astype(str).str.strip().str[:30]

        # Limpiar edades anómalas
        df['edad'] = df['edad'].apply(lambda x: int(x) if x is not None and x < 130 else 0)
        df['anno'] = df['anno'].apply(lambda x: int(x) if x is not None and 1900 <= x <= 2100 else 0)
        df = df.where(pd.notnull(df), None)  # Convertir NaN a None
        # Generar valores a insertar
        values = [tuple(row) for row in df.itertuples(index=False)]
        chunk_size = 1000
        chunks = list( self.util.dividir_en_chunks(values, chunk_size))

        conn = self._conectar()
        cursor = conn.cursor()

        query = """
        INSERT INTO dwh.base_personas_accidentes_tb
        (rol, tipo_lesion, edad, sexo, vehiculo, dia_semana, mes_anno, anno, provincia, canton, distrito, edad_quinquenal)
        VALUES %s
        """

        print("Iniciando inserción de datos...")
        for chunk in tqdm(chunks, desc="Insertando registros", ncols=100):
            try:
                execute_values(cursor, query, chunk)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print("Error al insertar chunk. Intentando fila por fila...")
                for row in chunk:
                    try:
                        execute_values(cursor, query, [row])
                        conn.commit()
                    except Exception as row_e:
                        conn.rollback()
                        print("Fila con error:")
                        print(row)
                        print("Error específico:")
                        print(row_e)
            except Exception as row_e:
                conn.rollback()
                print("Fila con error:")
                print(row)
                print("Tipos:", [type(col) for col in row])
                print("Error específico:")
                print(row_e)

        print("Datos insertados exitosamente en base_personas_accidentes_2017_2023.")

        cursor.close()
        conn.close()

    def carga_ubicaciones(self):
        conn = None
        cursor = None
        try:
            conn = self._conectar()
            conn.autocommit = False  # Necesario para usar cursores
            cursor = conn.cursor()

            # Definir nombre del cursor
            cursor_name = 'cursor_ubicaciones'

            # Llamar al procedimiento con el nombre del cursor
            cursor.execute(f"CALL dwh.sp_procesa_carga_ubicaciones('{cursor_name}');")

            # Leer los resultados
            cursor.execute(f"FETCH ALL FROM {cursor_name};")
            registros = cursor.fetchall()

            # Obtener nombres de columnas
            columnas = [desc[0] for desc in cursor.description]

            # Cargar a DataFrame
            df = pd.DataFrame(registros, columns=columnas)

            # Cerrar el cursor del lado de PostgreSQL
            cursor.execute(f"CLOSE {cursor_name};")

            conn.commit()
            return df

        except (psycopg2.DatabaseError, Exception) as error:
            print("❌ Error al ejecutar el procedimiento:", error)
            if conn:
                conn.rollback()
            return pd.DataFrame()

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def actualizar_ubicaciones(self,id, longitud, latitud):
        conn = None
        cursor = None
        try:
            conn = self._conectar()
            cursor = conn.cursor()

            # Consulta SQL para actualizar coordenadas
            query = """
                    UPDATE dwh.ubicaciones_tb
                    SET latitud = %s, longitud = %s
                    WHERE id = %s
                """

            cursor.execute(query, (latitud, longitud, id))
            conn.commit()
            print(f"Ubicación  actualizada exitosamente.")

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al actualizar la ubicación ': {e.pgerror}")

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def obtener_df_ubicaciones(self):
        conn = None
        cursor = None
        try:
            # Función que debes definir tú con tus datos de conexión
            conn = self._conectar()
            cursor = conn.cursor()

            # Consulta SQL
            query = """
                   SELECT id, provincia, canton, distrito, latitud, longitud
                   FROM dwh.ubicaciones_tb
                   where latitud is not null and
    			   (provincia, canton, distrito) not in(SELECT provincia, canton, distrito FROM dwh.lluvia_historica_tb);
                """

            cursor.execute(query)
            resultados = cursor.fetchall()

            # Obtener nombres de columnas
            columnas = [desc[0] for desc in cursor.description]

            # Convertir a DataFrame
            df = pd.DataFrame(resultados, columns=columnas)
            return df

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al consultar la base de datos: {e.pgerror}")
            return None

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def insertar_lluvia(self,provincia, canton, distrito, dia_semana, mes, anio, hora, lluvia_acumulada):
        conn = None
        cursor = None
        try:
            conn = self._conectar()
            cursor = conn.cursor()

            query = """
                INSERT INTO dwh.lluvia_historica_tb (
                    provincia, canton, distrito, dia_semana,
                    mes, anio, hora, lluvia_acumulada
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(query, (
                provincia, canton, distrito, dia_semana,
                mes, anio, hora, lluvia_acumulada
            ))

            conn.commit()
            print("Registro de lluvia insertado exitosamente.")

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al insertar el registro: {e.pgerror}")

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def obtener_df_modelo(self):
        conn = None
        cursor = None
        try:
            # Función que debes definir tú con tus datos de conexión
            conn = self._conectar()
            cursor = conn.cursor()

            # Consulta SQL
            query = """
                       SELECT * FROM dwh.modelo_ml_view;
                    """

            cursor.execute(query)
            resultados = cursor.fetchall()

            # Obtener nombres de columnas
            columnas = [desc[0] for desc in cursor.description]

            # Convertir a DataFrame
            df = pd.DataFrame(resultados, columns=columnas)
            return df

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al consultar la base de datos: {e.pgerror}")
            return None

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def obtener_list_provincias(self):
        conn = None
        cursor = None
        try:
            # Función que debes definir tú con tus datos de conexión
            conn = self._conectar()
            cursor = conn.cursor()

            # Consulta SQL
            query = """
                      SELECT distinct provincia FROM dwh.ubicaciones_tb
                        ORDER BY 1 ASC;
                        """

            cursor.execute(query)
            provincias = [fila[0] for fila in cursor.fetchall()]
            return provincias

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al consultar la base de datos: {e.pgerror}")
            return None

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def obtener_list_cantones(self,provincia):
        conn = None
        cursor = None
        try:
            # Función que debes definir tú con tus datos de conexión
            conn = self._conectar()
            cursor = conn.cursor()

            # Consulta SQL
            query = """
                      SELECT distinct canton FROM dwh.ubicaciones_tb
                        where provincia = %s
                        ORDER BY 1 ASC;
                        """

            cursor.execute(query, (provincia,))
            cantones = [fila[0] for fila in cursor.fetchall()]
            return cantones

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al consultar la base de datos: {e.pgerror}")
            return None

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def obtener_list_distritos(self,provincia, canton):
        conn = None
        cursor = None
        try:
            # Función que debes definir tú con tus datos de conexión
            conn = self._conectar()
            cursor = conn.cursor()

            # Consulta SQL
            query = """
                      SELECT distinct distrito FROM dwh.ubicaciones_tb
                        where provincia = %s and canton = %s
                        ORDER BY 1 ASC 
                        """

            cursor.execute(query, (provincia, canton))
            distritos = [fila[0] for fila in cursor.fetchall()]
            return distritos

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al consultar la base de datos: {e.pgerror}")
            return None

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def obtener_df_victimas(self):
        conn = None
        cursor = None
        try:
            # Función que debes definir tú con tus datos de conexión
            conn = self._conectar()
            cursor = conn.cursor()

            # Consulta SQL
            query = """
                              SELECT * FROM dwh.accidentes_victimas_tb;
                           """

            cursor.execute(query)
            resultados = cursor.fetchall()

            # Obtener nombres de columnas
            columnas = [desc[0] for desc in cursor.description]

            # Convertir a DataFrame
            df = pd.DataFrame(resultados, columns=columnas)
            return df

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al consultar la base de datos: {e.pgerror}")
            return None

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def obtener_df_personas(self):
        conn = None
        cursor = None
        try:
            # Función que debes definir tú con tus datos de conexión
            conn = self._conectar()
            cursor = conn.cursor()

            # Consulta SQL
            query = """
                              SELECT * FROM dwh.base_personas_accidentes_tb;
                           """

            cursor.execute(query)
            resultados = cursor.fetchall()

            # Obtener nombres de columnas
            columnas = [desc[0] for desc in cursor.description]

            # Convertir a DataFrame
            df = pd.DataFrame(resultados, columns=columnas)
            return df

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al consultar la base de datos: {e.pgerror}")
            return None

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def obtener_df_map_victimas(self):
        conn = None
        cursor = None
        try:
            # Función que debes definir tú con tus datos de conexión
            conn = self._conectar()
            cursor = conn.cursor()

            # Consulta SQL
            query = """
                 select
                    a.clase_accidente,
                    a.tipo_accidente,
                    a.dia_semana,
                    a.mes_anno,
                    a.anno,
                    a.franja_horaria_inicio,
                    a.franja_horaria_fin,
                    a.provincia,
                    a.canton,
                    a.distrito,
                    a.ruta,
                    a.kilometro,
                    a.tipo_ruta,
                    a.rural_urbano,
                    a.region_mideplan,
                    a.calzada_vertical,
                    a.calzada_horizontal,
                    a.tipo_calzada,
                    a.estado_calzada,
                    a.tipo_circulacion,
                    a.estado_tiempo,
                    ut.latitud,
                    ut.longitud
                from
                    dwh.accidentes_victimas_tb a
                inner join dwh.ubicaciones_tb ut on
                    ut.provincia = a.provincia and ut.canton = a.canton and ut.distrito = a.distrito and ut.latitud is not null
                                   """

            cursor.execute(query)
            resultados = cursor.fetchall()

            # Obtener nombres de columnas
            columnas = [desc[0] for desc in cursor.description]

            # Convertir a DataFrame
            df = pd.DataFrame(resultados, columns=columnas)
            return df

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al consultar la base de datos: {e.pgerror}")
            return None

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def obtener_df_map_personas(self):
        conn = None
        cursor = None
        try:
            # Función que debes definir tú con tus datos de conexión
            conn = self._conectar()
            cursor = conn.cursor()

            # Consulta SQL
            query = """
                        select
                            a.rol,
                            a.tipo_lesion,
                            a.edad,
                            a.sexo,
                            a.vehiculo,
                            a.dia_semana,
                            a.mes_anno,
                            a.anno,
                            a.provincia,
                            a.canton,
                            a.distrito,
                            a.edad_quinquenal,
                            ut.latitud,
                            ut.longitud
                        from
                            dwh.base_personas_accidentes_tb a
                            inner join dwh.ubicaciones_tb ut on
                            ut.provincia = a.provincia and ut.canton = a.canton and ut.distrito = a.distrito and ut.latitud is not null
                                          """

            cursor.execute(query)
            resultados = cursor.fetchall()

            # Obtener nombres de columnas
            columnas = [desc[0] for desc in cursor.description]

            # Convertir a DataFrame
            df = pd.DataFrame(resultados, columns=columnas)
            return df

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al consultar la base de datos: {e.pgerror}")
            return None

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def validar_data_cargada(self):
        conn = None
        cursor = None
        try:
            conn = self._conectar()
            cursor = conn.cursor()

            tablas = [
                "dwh.accidentes_victimas_tb",
                "dwh.ubicaciones_tb",
                "dwh.base_personas_accidentes_tb"
            ]

            for tabla in tablas:
                cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                count = cursor.fetchone()[0]
                if count == 0:
                    return True  # Si alguna está vacía, retorna True

            return False  # Todas tienen datos

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            print(f"Error al consultar la base de datos: {e.pgerror}")
            return True  # Ante error, asumir que están vacías por seguridad

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inesperado: {e}")
            return True

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
import os
from pathlib import Path

import joblib
import pandas as pd
import io
import sys
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import KFold, cross_val_score
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from imblearn.over_sampling import SMOTE

from src.basedatos.GestorBaseDatos import GestorBaseDatos


class ModeloML:

    def __init__(self):
        self.bd = GestorBaseDatos()

    def _definir_columnas(self):
        columnas_categoricas = ['provincia', 'canton', 'distrito', 'tipo_ruta', 'dia_semana']
        columnas_numericas = ['hora', 'lluvia_acumulada']
        return columnas_categoricas, columnas_numericas

    # Construye el preprocesador con imputación y escalado
    def _construir_preprocesador(self):
        cat_cols, num_cols = self._definir_columnas()
        preprocessor = ColumnTransformer(
            transformers=[
                ('cate', Pipeline([
                    ('imputer', SimpleImputer(strategy='constant', fill_value='missing', keep_empty_features=True)),
                    ('onehot', OneHotEncoder(handle_unknown='ignore'))
                ]), cat_cols),
                ('num', Pipeline([
                    ('imputer', SimpleImputer(strategy='constant', fill_value=0, keep_empty_features=True)),
                    ('scaler', StandardScaler())
                ]), num_cols)
            ],
            remainder='drop'
        )
        return preprocessor

    # Analiza y elige el mejor modelo con validación cruzada
    def _analisis_modelo(self,X, y):
        preprocessor = self._construir_preprocesador()
        candidatos = [
            ('LR', LogisticRegression(class_weight='balanced', max_iter=1000)),
            ('CART', DecisionTreeClassifier(class_weight='balanced'))
        ]
        mejor_nombre, mejor_puntaje = None, 0
        print("Evaluando modelos con imputación y escalado...")
        for nombre, clasificador in candidatos:
            pipeline = Pipeline([('pre', preprocessor), ('clf', clasificador)])
            kf = KFold(n_splits=10, shuffle=True, random_state=42)
            try:
                resultados = cross_val_score(pipeline, X, y, cv=kf, error_score='raise')
                promedio, desviacion = resultados.mean(), resultados.std()
                print(f"{nombre}: {promedio:.4f} ± {desviacion:.4f}")
                if promedio > mejor_puntaje:
                    mejor_puntaje, mejor_nombre = promedio, nombre
            except Exception as e:
                print(f"Error en {nombre}: {e}")
        if mejor_nombre is None:
            raise RuntimeError("No se pudo determinar el mejor modelo, revisar datos e imputación.")
        print(f"\nMejor modelo: {mejor_nombre} con puntaje {mejor_puntaje:.4f}")
        return mejor_nombre

    # Entrena el pipeline definitivo
    def _entrenar_modelo(self,nombre_modelo, X, y):
        preprocessor = self._construir_preprocesador()

        if nombre_modelo == 'LR':
            clasificador = LogisticRegression(class_weight='balanced', max_iter=1000)
        elif nombre_modelo == 'CART':
            clasificador = DecisionTreeClassifier(class_weight='balanced')
        else:
            raise ValueError(f"Modelo desconocido: {nombre_modelo}")

        # 1. Aplicar preprocesamiento
        X_proc = preprocessor.fit_transform(X)

        # 2. Aplicar SMOTE
        smote = SMOTE(random_state=42)
        X_bal, y_bal = smote.fit_resample(X_proc, y)

        # 3. Entrenar clasificador con los datos balanceados
        clasificador.fit(X_bal, y_bal)

        # 4. Construir pipeline completo para predicción futura
        pipeline = Pipeline([
            ('pre', preprocessor),
            ('clf', clasificador)
        ])

        # 5. Evaluar resultados en los datos originales (no balanceados)
        y_pred = pipeline.predict(X)
        print("\nReporte de clasificación en entrenamiento:")
        print(classification_report(y, y_pred))
        print("Precisión global:", accuracy_score(y, y_pred))

        print("Matriz de confusión:")
        print(confusion_matrix(y, y_pred))

        return pipeline

    # Función principal: obtiene datos, analiza y entrena
    def procesar_modelo_ml(self):
        buffer = io.StringIO()
        sys_stdout_original = sys.stdout  # Guardar stdout actual
        sys.stdout = buffer  # Redirigir print() al buffer

        # Ruta base del proyecto (2 niveles arriba del archivo actual)
        BASE_DIR = Path(__file__).resolve().parents[2]
        modelo_path = BASE_DIR / "data" / "processed" / "modelo_cosevi.pkl"

        # Verificar si el modelo ya existe
        if os.path.exists(modelo_path):
            modelo = joblib.load(modelo_path)
            log =  f"Modelo cargado desde: {modelo_path}"
            return modelo, log

        try:
            df = self.bd.obtener_df_modelo()
            if 'accidente' not in df.columns:
                raise ValueError("Falta la columna 'accidente' en el DataFrame")
            # Selección de columnas
            cat_cols, num_cols = self._definir_columnas()
            X = df[cat_cols + num_cols].copy()
            y = df['accidente']
            # Convertir hora: string a número, crea NaN si falla
            X['hora'] = pd.to_datetime(X['hora'], errors='coerce').dt.hour
            X['hora'] = pd.to_numeric(X['hora'], errors='coerce')
            mejor = self._analisis_modelo(X, y)
            modelo = self._entrenar_modelo(mejor, X, y)
            # Guardar el modelo entrenado
            joblib.dump(modelo, modelo_path)
        finally:
            sys.stdout = sys_stdout_original  # Restaurar stdout

        log = buffer.getvalue()
        return modelo, log

    # Predice nuevos casos con robustez a valores faltantes
    def predecir_nuevo(self,modelo_entrenado, provincia, canton, distrito, tipo_ruta,
                       hora, dia_semana, lluvia_acumulada):
        cat_cols, num_cols = self._definir_columnas()
        datos = {
            'provincia': provincia,
            'canton': canton,
            'distrito': distrito,
            'tipo_ruta': tipo_ruta,
            'dia_semana': dia_semana,
            'lluvia_acumulada': lluvia_acumulada,
            'hora': hora
        }
        df_nuevo = pd.DataFrame([datos])
        # Convertir hora: string a número
        df_nuevo['hora'] = pd.to_datetime(df_nuevo['hora'], errors='coerce').dt.hour
        df_nuevo['hora'] = pd.to_numeric(df_nuevo['hora'], errors='coerce')
        # Forzar tipos
        df_nuevo[cat_cols] = df_nuevo[cat_cols].astype(str)
        df_nuevo[num_cols] = df_nuevo[num_cols].astype(float)

        # Predicción
        pred = modelo_entrenado.predict(df_nuevo)[0]
        prob = modelo_entrenado.predict_proba(df_nuevo)[0]

        # Obtener clases para saber a qué corresponde cada probabilidad
        clases = modelo_entrenado.classes_

        # print(f"¿Ocurre accidente? {pred}")
        for clase, p in zip(clases, prob):
            print(f"Probabilidad de {clase}: {p*100:.4f}")

        return {'prediccion': pred, 'probabilidad': dict(zip(clases, prob))}
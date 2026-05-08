import os
import logging
import pandas as pd

RAW_PATH = "data/raw/telco_customer_churn.csv"
PROCESSED_DIR = "data/processed"
PROCESSED_PATH = os.path.join(PROCESSED_DIR, "telco_customer_churn_clean.csv")

LOGS_DIR = "logs"
LOG_PATH = os.path.join(LOGS_DIR, "limpieza_transformacion.log")


def configurar_logging():
    os.makedirs(LOGS_DIR, exist_ok=True)
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True
    )


def clasificar_tenure(meses):
    if meses <= 12:
        return "Nuevo"
    elif meses <= 48:
        return "Intermedio"
    else:
        return "Antiguo"


def limpiar_transformar():
    configurar_logging()

    try:
        if not os.path.exists(RAW_PATH):
            raise FileNotFoundError(f"No se encontró el archivo: {RAW_PATH}")

        os.makedirs(PROCESSED_DIR, exist_ok=True)

        # 1. Cargar dataset
        df = pd.read_csv(RAW_PATH)

        if df.empty:
            raise ValueError("El dataset en raw está vacío.")

        filas_iniciales, columnas_iniciales = df.shape

        # 2. Inspección inicial
        print("Columnas:")
        print(df.columns.tolist())

        print("\nTipos de datos:")
        print(df.dtypes)

        print("\nValores faltantes por columna:")
        print(df.isna().sum())

        print("\nCantidad de duplicados:")
        print(df.duplicated().sum())

        print("\nCustomerID duplicados:")
        print(df["customerID"].duplicated().sum())

        logging.info("Inspección inicial completada.")
        logging.info(f"Filas iniciales: {filas_iniciales} | Columnas iniciales: {columnas_iniciales}")
        logging.info(f"Columnas: {df.columns.tolist()}")
        logging.info(f"Tipos de datos iniciales:\n{df.dtypes}")
        logging.info(f"Valores faltantes iniciales:\n{df.isna().sum()}")
        logging.info(f"Duplicados iniciales: {df.duplicated().sum()}")
        logging.info(f"CustomerID duplicados iniciales: {df['customerID'].duplicated().sum()}")

        # 3. Limpieza y transformación

        # 3.1 Eliminar espacios en columnas de texto
        columnas_objeto = df.select_dtypes(include="object").columns
        for col in columnas_objeto:
            df[col] = df[col].astype(str).str.strip()

        logging.info("Se eliminaron espacios en columnas de texto.")

        # 3.2 Convertir TotalCharges a numérico
        if "TotalCharges" in df.columns:
            df["TotalCharges"] = df["TotalCharges"].replace("", pd.NA)
            df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

        logging.info("Se convirtió TotalCharges a numérico, forzando errores a valores nulos.")

        # 3.3 Imputar faltantes de TotalCharges con mediana
        faltantes_totalcharges_antes = df["TotalCharges"].isna().sum() if "TotalCharges" in df.columns else 0

        if "TotalCharges" in df.columns and df["TotalCharges"].isna().sum() > 0:
            mediana_totalcharges = df["TotalCharges"].median()
            df["TotalCharges"] = df["TotalCharges"].fillna(mediana_totalcharges)

        faltantes_totalcharges_despues = df["TotalCharges"].isna().sum() if "TotalCharges" in df.columns else 0

        logging.info(
            f"Faltantes en TotalCharges antes: {faltantes_totalcharges_antes} | "
            f"después: {faltantes_totalcharges_despues}"
        )

        # 3.4 Transformar SeniorCitizen a categoría legible
        if "SeniorCitizen" in df.columns:
            df["SeniorCitizen"] = df["SeniorCitizen"].replace({0: "No", 1: "Si"})

        logging.info("Se transformó SeniorCitizen de binario a categoría legible.")

        # 3.5 Validar rangos lógicos
        if "tenure" in df.columns:
            filas_antes = len(df)
            df = df[df["tenure"] >= 0]
            logging.info(f"Registros removidos por tenure negativo: {filas_antes - len(df)}")

        if "MonthlyCharges" in df.columns:
            filas_antes = len(df)
            df = df[df["MonthlyCharges"] >= 0]
            logging.info(f"Registros removidos por MonthlyCharges negativo: {filas_antes - len(df)}")

        if "TotalCharges" in df.columns:
            filas_antes = len(df)
            df = df[df["TotalCharges"] >= 0]
            logging.info(f"Registros removidos por TotalCharges negativo: {filas_antes - len(df)}")

        # 3.6 Crear variable derivada Tenure_Group
        if "tenure" in df.columns:
            df["Tenure_Group"] = df["tenure"].apply(clasificar_tenure)

        logging.info("Se creó la variable derivada Tenure_Group.")

        # 4. Inspección final
        print("\nTipos de datos después de la limpieza:")
        print(df.dtypes)

        print("\nValores faltantes después de la limpieza:")
        print(df.isna().sum())

        print("\nCantidad de duplicados después de la limpieza:")
        print(df.duplicated().sum())

        print("\nCustomerID duplicados después de la limpieza:")
        print(df["customerID"].duplicated().sum())

        logging.info(f"Tipos de datos finales:\n{df.dtypes}")
        logging.info(f"Valores faltantes finales:\n{df.isna().sum()}")
        logging.info(f"Duplicados finales: {df.duplicated().sum()}")
        logging.info(f"CustomerID duplicados finales: {df['customerID'].duplicated().sum()}")

        # 5. Guardar resultado
        df.to_csv(PROCESSED_PATH, index=False)

        filas_finales, columnas_finales = df.shape

        logging.info("Limpieza y transformación completadas correctamente.")
        logging.info(f"Archivo guardado en: {PROCESSED_PATH}")
        logging.info(f"Filas finales: {filas_finales} | Columnas finales: {columnas_finales}")

        print("\nLimpieza y transformación completadas correctamente.")
        print(f"Archivo guardado en: {PROCESSED_PATH}")
        print(f"Filas finales: {filas_finales} | Columnas finales: {columnas_finales}")
        print(f"Log generado en: {LOG_PATH}")

    except Exception as e:
        logging.error(f"Error durante la limpieza y transformación: {e}")
        print(f"Error durante la limpieza y transformación: {e}")


if __name__ == "__main__":
    limpiar_transformar()
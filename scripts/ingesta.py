import os
import logging
import pandas as pd

SOURCE_PATH = "data/source/02_Base_WA_Fn-UseC_-Telco-Customer-Churn.csv"
RAW_DIR = "data/raw"
RAW_PATH = os.path.join(RAW_DIR, "telco_customer_churn.csv")

LOGS_DIR = "logs"
LOG_PATH = os.path.join(LOGS_DIR, "ingesta.log")


def configurar_logging():
    os.makedirs(LOGS_DIR, exist_ok=True)
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True
    )


def ingestar_datos():
    configurar_logging()

    try:
        # Verificar que exista el archivo fuente
        if not os.path.exists(SOURCE_PATH):
            raise FileNotFoundError(f"No se encontró el archivo fuente: {SOURCE_PATH}")

        # Crear carpeta raw si no existe
        os.makedirs(RAW_DIR, exist_ok=True)

        # Leer dataset
        df = pd.read_csv(SOURCE_PATH)

        # Verificar que no esté vacío
        if df.empty:
            raise ValueError("El dataset fuente está vacío.")

        # Guardar copia en raw
        df.to_csv(RAW_PATH, index=False)

        logging.info("Ingesta completada correctamente.")
        logging.info(f"Archivo fuente: {SOURCE_PATH}")
        logging.info(f"Archivo guardado en: {RAW_PATH}")
        logging.info(f"Filas: {df.shape[0]} | Columnas: {df.shape[1]}")

        print("Ingesta completada correctamente.")
        print(f"Archivo fuente: {SOURCE_PATH}")
        print(f"Archivo guardado en: {RAW_PATH}")
        print(f"Filas: {df.shape[0]} | Columnas: {df.shape[1]}")
        print(f"Log generado en: {LOG_PATH}")

    except Exception as e:
        logging.error(f"Error durante la ingesta: {e}")
        print(f"Error durante la ingesta: {e}")


if __name__ == "__main__":
    ingestar_datos()
import os
import logging
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

VALIDATED_PATH = "data/validated/telco_customer_churn_validated.csv"

INSERTED_DIR = "data/inserted"
REJECTED_DIR = "data/rejected"
INSERTED_PATH = os.path.join(INSERTED_DIR, "inserted_records.csv")
REJECTED_PATH = os.path.join(REJECTED_DIR, "rejected_records.csv")

LOGS_DIR = "logs"
LOG_PATH = os.path.join(LOGS_DIR, "carga_bd.log")

DB_CONFIG = {
    "host": os.getenv("PGHOST"),
    "port": int(os.getenv("PGPORT", 5432)),
    "dbname": os.getenv("PGDATABASE"),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD")
}


def configurar_logging():
    os.makedirs(LOGS_DIR, exist_ok=True)
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True
    )


def crear_tabla(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telco_churn_cases (
            customerID VARCHAR(50) PRIMARY KEY,
            gender VARCHAR(20) NOT NULL,
            SeniorCitizen VARCHAR(10) NOT NULL,
            Partner VARCHAR(10) NOT NULL,
            Dependents VARCHAR(10) NOT NULL,
            tenure INTEGER NOT NULL,
            PhoneService VARCHAR(10) NOT NULL,
            MultipleLines VARCHAR(30) NOT NULL,
            InternetService VARCHAR(30) NOT NULL,
            OnlineSecurity VARCHAR(30) NOT NULL,
            OnlineBackup VARCHAR(30) NOT NULL,
            DeviceProtection VARCHAR(30) NOT NULL,
            TechSupport VARCHAR(30) NOT NULL,
            StreamingTV VARCHAR(30) NOT NULL,
            StreamingMovies VARCHAR(30) NOT NULL,
            Contract VARCHAR(30) NOT NULL,
            PaperlessBilling VARCHAR(10) NOT NULL,
            PaymentMethod VARCHAR(50) NOT NULL,
            MonthlyCharges DOUBLE PRECISION NOT NULL,
            TotalCharges DOUBLE PRECISION NOT NULL,
            Churn VARCHAR(10) NOT NULL,
            Tenure_Group VARCHAR(20) NOT NULL
        )
    """)

    conn.commit()
    cursor.close()


def validar_registro(row):
    errores = []

    if pd.isna(row["customerID"]) or str(row["customerID"]).strip() == "":
        errores.append("customerID nulo o vacío")

    if row["tenure"] < 0:
        errores.append("tenure negativo")

    if row["MonthlyCharges"] < 0:
        errores.append("MonthlyCharges negativo")

    if row["TotalCharges"] < 0:
        errores.append("TotalCharges negativo")

    if row["gender"] not in {"Male", "Female"}:
        errores.append("gender inválido")

    if row["SeniorCitizen"] not in {"Si", "No"}:
        errores.append("SeniorCitizen inválido")

    if row["Partner"] not in {"Yes", "No"}:
        errores.append("Partner inválido")

    if row["Dependents"] not in {"Yes", "No"}:
        errores.append("Dependents inválido")

    if row["PhoneService"] not in {"Yes", "No"}:
        errores.append("PhoneService inválido")

    if row["PaperlessBilling"] not in {"Yes", "No"}:
        errores.append("PaperlessBilling inválido")

    if row["Churn"] not in {"Yes", "No"}:
        errores.append("Churn inválido")

    if row["Tenure_Group"] not in {"Nuevo", "Intermedio", "Antiguo"}:
        errores.append("Tenure_Group inválido")

    tenure = row["tenure"]
    grupo = row["Tenure_Group"]

    if tenure <= 12 and grupo != "Nuevo":
        errores.append("Inconsistencia tenure y Tenure_Group")
    elif 13 <= tenure <= 48 and grupo != "Intermedio":
        errores.append("Inconsistencia tenure y Tenure_Group")
    elif tenure > 48 and grupo != "Antiguo":
        errores.append("Inconsistencia tenure y Tenure_Group")

    return errores


def cargar_datos():
    configurar_logging()

    try:
        if not os.path.exists(VALIDATED_PATH):
            raise FileNotFoundError(f"No se encontró el archivo: {VALIDATED_PATH}")

        os.makedirs(INSERTED_DIR, exist_ok=True)
        os.makedirs(REJECTED_DIR, exist_ok=True)

        df = pd.read_csv(VALIDATED_PATH)

        if df.empty:
            raise ValueError("El dataset validado está vacío.")

        logging.info("Dataset validado cargado correctamente.")
        logging.info(f"Filas leídas: {df.shape[0]} | Columnas leídas: {df.shape[1]}")

        conn = psycopg2.connect(**DB_CONFIG)
        crear_tabla(conn)
        cursor = conn.cursor()

        inserted_records = []
        rejected_records = []
        skipped_records = []

        for _, row in df.iterrows():
            errores = validar_registro(row)

            if errores:
                rejected_row = row.to_dict()
                rejected_row["error_description"] = " | ".join(errores)
                rejected_records.append(rejected_row)
                logging.warning(
                    f"Registro rechazado customerID={row.get('customerID')}: {' | '.join(errores)}"
                )
                continue

            try:
                cursor.execute("""
                    INSERT INTO telco_churn_cases (
                        customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
                        PhoneService, MultipleLines, InternetService, OnlineSecurity,
                        OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
                        StreamingMovies, Contract, PaperlessBilling, PaymentMethod,
                        MonthlyCharges, TotalCharges, Churn, Tenure_Group
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customerID) DO NOTHING
                """, (
                    str(row["customerID"]),
                    str(row["gender"]),
                    str(row["SeniorCitizen"]),
                    str(row["Partner"]),
                    str(row["Dependents"]),
                    int(row["tenure"]),
                    str(row["PhoneService"]),
                    str(row["MultipleLines"]),
                    str(row["InternetService"]),
                    str(row["OnlineSecurity"]),
                    str(row["OnlineBackup"]),
                    str(row["DeviceProtection"]),
                    str(row["TechSupport"]),
                    str(row["StreamingTV"]),
                    str(row["StreamingMovies"]),
                    str(row["Contract"]),
                    str(row["PaperlessBilling"]),
                    str(row["PaymentMethod"]),
                    float(row["MonthlyCharges"]),
                    float(row["TotalCharges"]),
                    str(row["Churn"]),
                    str(row["Tenure_Group"]),
                ))

                if cursor.rowcount == 1:
                    inserted_records.append(row.to_dict())
                else:
                    skipped_row = row.to_dict()
                    skipped_row["info"] = "Registro ya existente, omitido por conflicto de primary key."
                    skipped_records.append(skipped_row)

            except psycopg2.IntegrityError as e:
                conn.rollback()
                rejected_row = row.to_dict()
                rejected_row["error_description"] = str(e)
                rejected_records.append(rejected_row)
                logging.error(
                    f"Error de integridad en customerID={row.get('customerID')}: {e}"
                )
            except Exception as e:
                conn.rollback()
                rejected_row = row.to_dict()
                rejected_row["error_description"] = str(e)
                rejected_records.append(rejected_row)
                logging.error(
                    f"Error al insertar customerID={row.get('customerID')}: {e}"
                )

        conn.commit()
        cursor.close()
        conn.close()

        pd.DataFrame(inserted_records).to_csv(INSERTED_PATH, index=False)
        pd.DataFrame(rejected_records).to_csv(REJECTED_PATH, index=False)

        logging.info(f"Registros insertados: {len(inserted_records)}")
        logging.info(f"Registros rechazados: {len(rejected_records)}")
        logging.info(f"Registros omitidos por conflicto: {len(skipped_records)}")
        logging.info("Carga a base de datos completada correctamente.")

        print("Carga a base de datos completada correctamente.")
        print("Motor utilizado: PostgreSQL")
        print(f"Base de datos destino: {DB_CONFIG['dbname']}")
        print("Tabla destino: telco_churn_cases")
        print(f"Registros insertados: {len(inserted_records)}")
        print(f"Registros rechazados: {len(rejected_records)}")
        print(f"Registros omitidos por conflicto: {len(skipped_records)}")
        print(f"Archivo de insertados: {INSERTED_PATH}")
        print(f"Archivo de rechazados: {REJECTED_PATH}")
        print(f"Log generado en: {LOG_PATH}")

    except Exception as e:
        logging.error(f"Error durante la carga: {e}")
        print(f"Error durante la carga: {e}")


if __name__ == "__main__":
    cargar_datos()
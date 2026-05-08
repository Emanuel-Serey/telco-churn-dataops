import os
import logging
import pandas as pd

PROCESSED_PATH = "data/processed/telco_customer_churn_clean.csv"
VALIDATED_DIR = "data/validated"
VALIDATED_PATH = os.path.join(VALIDATED_DIR, "telco_customer_churn_validated.csv")

REPORTS_DIR = "data/reports"
REPORT_PATH = os.path.join(REPORTS_DIR, "validation_report.csv")

LOGS_DIR = "logs"
LOG_PATH = os.path.join(LOGS_DIR, "validacion_datos.log")


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


def validar_datos():
    configurar_logging()

    try:
        if not os.path.exists(PROCESSED_PATH):
            raise FileNotFoundError(f"No se encontró el archivo: {PROCESSED_PATH}")

        os.makedirs(VALIDATED_DIR, exist_ok=True)
        os.makedirs(REPORTS_DIR, exist_ok=True)

        df = pd.read_csv(PROCESSED_PATH)

        if df.empty:
            raise ValueError("El dataset procesado está vacío.")

        logging.info("Inicio de validación estructural y semántica.")
        logging.info(f"Filas leídas: {df.shape[0]} | Columnas leídas: {df.shape[1]}")

        errores = []

        columnas_obligatorias = [
            "customerID", "gender", "SeniorCitizen", "Partner", "Dependents",
            "tenure", "PhoneService", "MultipleLines", "InternetService",
            "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport",
            "StreamingTV", "StreamingMovies", "Contract", "PaperlessBilling",
            "PaymentMethod", "MonthlyCharges", "TotalCharges", "Churn", "Tenure_Group"
        ]

        # 1. Validación de columnas obligatorias
        for col in columnas_obligatorias:
            if col not in df.columns:
                errores.append({
                    "tipo_error": "columna_faltante",
                    "columna": col,
                    "detalle": f"La columna {col} no está presente en el dataset."
                })

        # 2. Validación de customerID
        if "customerID" in df.columns:
            if df["customerID"].isna().sum() > 0:
                errores.append({
                    "tipo_error": "customerID_nulo",
                    "columna": "customerID",
                    "detalle": "Existen customerID nulos."
                })

            duplicados_id = df["customerID"].duplicated().sum()
            if duplicados_id > 0:
                errores.append({
                    "tipo_error": "customerID_duplicado",
                    "columna": "customerID",
                    "detalle": f"Existen {duplicados_id} customerID duplicados."
                })

        # 3. Validación de rangos numéricos
        for col in ["tenure", "MonthlyCharges", "TotalCharges"]:
            if col in df.columns:
                negativos = (df[col] < 0).sum()
                if negativos > 0:
                    errores.append({
                        "tipo_error": "valor_negativo",
                        "columna": col,
                        "detalle": f"Existen {negativos} valores negativos en {col}."
                    })

        # 4. Validación de categorías permitidas
        valores_permitidos = {
            "gender": {"Male", "Female"},
            "SeniorCitizen": {"Si", "No"},
            "Partner": {"Yes", "No"},
            "Dependents": {"Yes", "No"},
            "PhoneService": {"Yes", "No"},
            "PaperlessBilling": {"Yes", "No"},
            "Churn": {"Yes", "No"},
            "Tenure_Group": {"Nuevo", "Intermedio", "Antiguo"},
        }

        for col, permitidos in valores_permitidos.items():
            if col in df.columns:
                invalidos = set(df[col].dropna().unique()) - permitidos
                if invalidos:
                    errores.append({
                        "tipo_error": "categoria_invalida",
                        "columna": col,
                        "detalle": f"Valores inválidos en {col}: {invalidos}"
                    })

        # 5. Validación semántica de Tenure_Group
        if "tenure" in df.columns and "Tenure_Group" in df.columns:
            inconsistencias = df[df["Tenure_Group"] != df["tenure"].apply(clasificar_tenure)]
            if len(inconsistencias) > 0:
                errores.append({
                    "tipo_error": "inconsistencia_tenure_group",
                    "columna": "Tenure_Group",
                    "detalle": f"Existen {len(inconsistencias)} inconsistencias entre tenure y Tenure_Group."
                })

        # 6. Guardar reporte
        if errores:
            reporte_df = pd.DataFrame(errores)
            logging.warning(f"Se detectaron {len(errores)} observaciones de validación.")
        else:
            reporte_df = pd.DataFrame([{
                "tipo_error": "sin_errores",
                "columna": "-",
                "detalle": "No se detectaron errores de validación."
            }])
            logging.info("No se detectaron errores de validación.")

        reporte_df.to_csv(REPORT_PATH, index=False)

        # 7. Guardar dataset validado
        df.to_csv(VALIDATED_PATH, index=False)

        logging.info("Validación completada correctamente.")
        logging.info(f"Reporte guardado en: {REPORT_PATH}")
        logging.info(f"Dataset validado guardado en: {VALIDATED_PATH}")

        print("Validación completada correctamente.")
        print(f"Reporte guardado en: {REPORT_PATH}")
        print(f"Dataset validado guardado en: {VALIDATED_PATH}")
        print(f"Log generado en: {LOG_PATH}")

    except Exception as e:
        logging.error(f"Error durante la validación: {e}")
        print(f"Error durante la validación: {e}")


if __name__ == "__main__":
    validar_datos()
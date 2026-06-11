import os
import logging
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
    roc_curve
)


# Configuración de carpetas

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "validated", "telco_customer_churn_validated.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# Configuración de logging

log_file = os.path.join(LOG_DIR, "modelo_final.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

print("[LOG] Iniciando pipeline del modelo final...")
logging.info("Iniciando pipeline del modelo final")


# Cargar datos

df = pd.read_csv(DATA_PATH)
print(f"[LOG] Dataset cargado correctamente: {df.shape}")
logging.info(f"Dataset cargado correctamente: {df.shape}")


# Feature engineering

df["AltoCosto_MesAMes"] = (
    (df["Contract"] == "Month-to-month") &
    (df["MonthlyCharges"] > 70)
).astype(int)

df["Charges_per_Tenure"] = df["TotalCharges"] / (df["tenure"] + 1)

df["MesAMes_x_Cargo"] = (
    (df["Contract"] == "Month-to-month").astype(int) * df["MonthlyCharges"]
)

print("[LOG] Variables derivadas creadas correctamente")
logging.info("Variables derivadas creadas correctamente")


# Selección de variables

X = df.drop(columns=["customerID", "Churn", "gender", "PhoneService"])
y = df["Churn"].map({"No": 0, "Yes": 1})

X_encoded = pd.get_dummies(X, drop_first=True)

print(f"[LOG] Variables codificadas: {X_encoded.shape}")
logging.info(f"Variables codificadas: {X_encoded.shape}")


# Train / test split

X_train, X_test, y_train, y_test = train_test_split(
    X_encoded,
    y,
    test_size=0.2,
    stratify=y,
    random_state=42
)

print("[LOG] División train/test realizada")
logging.info("División train/test realizada")


# Escalado

num_cols = ["tenure", "MonthlyCharges", "TotalCharges", "Charges_per_Tenure", "MesAMes_x_Cargo"]

X_train_scaled = X_train.copy()
X_test_scaled = X_test.copy()

scaler = MinMaxScaler()
X_train_scaled[num_cols] = scaler.fit_transform(X_train[num_cols])
X_test_scaled[num_cols] = scaler.transform(X_test[num_cols])

print("[LOG] Escalado aplicado correctamente")
logging.info("Escalado aplicado correctamente")


# Modelo final

modelo = LogisticRegression(
    C=0.1,
    penalty="l2",
    solver="liblinear",
    max_iter=3000,
    class_weight="balanced"
)

modelo.fit(X_train_scaled, y_train)

print("[LOG] Modelo entrenado correctamente")
logging.info("Modelo entrenado correctamente")


# Predicciones con threshold final

threshold = 0.55

y_prob = modelo.predict_proba(X_test_scaled)[:, 1]
y_pred = (y_prob >= threshold).astype(int)

print(f"[LOG] Predicciones generadas con threshold={threshold}")
logging.info(f"Predicciones generadas con threshold={threshold}")


# Métricas

accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)
roc_auc = roc_auc_score(y_test, y_prob)
gini = 2 * roc_auc - 1

tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
fpr, tpr, thresholds_roc = roc_curve(y_test, y_prob)

print("[LOG] Métricas calculadas correctamente")
logging.info("Métricas calculadas correctamente")


# Exportar KPIs

kpis_df = pd.DataFrame([{
    "Modelo": "Regresión Logística",
    "Threshold": threshold,
    "Accuracy": accuracy,
    "Precision": precision,
    "Recall": recall,
    "F1-score": f1,
    "ROC-AUC": roc_auc,
    "Gini": gini
}])

kpis_path = os.path.join(OUTPUT_DIR, "kpis_modelo_final.csv")
kpis_df.to_csv(kpis_path, index=False)

print(f"[LOG] KPIs exportados en: {kpis_path}")
logging.info(f"KPIs exportados en: {kpis_path}")


# Exportar matriz de confusión

matriz_confusion_df = pd.DataFrame([
    {"Tipo": "TN", "Valor": tn, "Descripcion": "No churn real y No churn predicho"},
    {"Tipo": "FP", "Valor": fp, "Descripcion": "No churn real y churn predicho"},
    {"Tipo": "FN", "Valor": fn, "Descripcion": "Churn real y No churn predicho"},
    {"Tipo": "TP", "Valor": tp, "Descripcion": "Churn real y churn predicho"}
])

matriz_path = os.path.join(OUTPUT_DIR, "matriz_confusion_modelo_final.csv")
matriz_confusion_df.to_csv(matriz_path, index=False)

print(f"[LOG] Matriz de confusión exportada en: {matriz_path}")
logging.info(f"Matriz de confusión exportada en: {matriz_path}")


# Exportar curva ROC

roc_df = pd.DataFrame({
    "FPR": fpr,
    "TPR": tpr,
    "Threshold_ROC": thresholds_roc
})

roc_path = os.path.join(OUTPUT_DIR, "curva_roc_modelo_final.csv")
roc_df.to_csv(roc_path, index=False)

print(f"[LOG] Curva ROC exportada en: {roc_path}")
logging.info(f"Curva ROC exportada en: {roc_path}")


# Exportar predicciones individuales

predicciones_df = X_test.copy()
predicciones_df["Churn_real"] = y_test.values
predicciones_df["Churn_predicho"] = y_pred
predicciones_df["Probabilidad_churn"] = y_prob

predicciones_df["Churn_real_label"] = predicciones_df["Churn_real"].map({0: "No", 1: "Yes"})
predicciones_df["Churn_predicho_label"] = predicciones_df["Churn_predicho"].map({0: "No", 1: "Yes"})

predicciones_df["Nivel_riesgo"] = pd.cut(
    predicciones_df["Probabilidad_churn"],
    bins=[0, 0.33, 0.66, 1],
    labels=["Bajo", "Medio", "Alto"],
    include_lowest=True
)

predicciones_path = os.path.join(OUTPUT_DIR, "predicciones_modelo_final.csv")
predicciones_df.to_csv(predicciones_path, index=False)

print(f"[LOG] Predicciones exportadas en: {predicciones_path}")
logging.info(f"Predicciones exportadas en: {predicciones_path}")

print("[LOG] Pipeline del modelo final completado correctamente")
logging.info("Pipeline del modelo final completado correctamente")
import os
import sys
import subprocess
import logging

# -----------------------------
# Rutas base
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

# -----------------------------
# Configuración de logging
# -----------------------------
log_file = os.path.join(LOG_DIR, "pipeline_completo.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def ejecutar_script(nombre_script):
    ruta_script = os.path.join(SCRIPTS_DIR, nombre_script)

    print(f"[LOG] Ejecutando: {nombre_script}")
    logging.info(f"Ejecutando: {nombre_script}")

    try:
        resultado = subprocess.run(
            [sys.executable, ruta_script],
            check=True,
            capture_output=True,
            text=True
        )

        if resultado.stdout:
            print(resultado.stdout)
            logging.info(resultado.stdout)

        if resultado.stderr:
            print(resultado.stderr)
            logging.warning(resultado.stderr)

        print(f"[LOG] {nombre_script} ejecutado correctamente")
        logging.info(f"{nombre_script} ejecutado correctamente")

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Falló la ejecución de {nombre_script}")
        logging.error(f"Falló la ejecución de {nombre_script}")

        if e.stdout:
            print(e.stdout)
            logging.error(e.stdout)

        if e.stderr:
            print(e.stderr)
            logging.error(e.stderr)

        raise


if __name__ == "__main__":
    print("[LOG] Iniciando pipeline completo...")
    logging.info("Iniciando pipeline completo")

    try:
        # -----------------------------
        # 1. Pipeline de datos
        # -----------------------------
        ejecutar_script("ingesta.py")
        ejecutar_script("limpieza_transformacion.py")
        ejecutar_script("validacion_datos.py")
        ejecutar_script("carga_bd.py")

        # -----------------------------
        # 2. Modelo final
        # -----------------------------
        ejecutar_script("modelo_final.py")

        print("[LOG] Pipeline completo ejecutado correctamente")
        logging.info("Pipeline completo ejecutado correctamente")

    except Exception as e:
        print(f"[ERROR] El pipeline completo falló: {str(e)}")
        logging.error(f"El pipeline completo falló: {str(e)}")
        raise
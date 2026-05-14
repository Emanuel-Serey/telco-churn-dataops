import subprocess
import sys

scripts = [
    "scripts/ingesta.py",
    "scripts/limpieza_transformacion.py",
    "scripts/validacion_datos.py",
    "scripts/carga_bd.py"
]

for script in scripts:
    print(f"\nEjecutando: {script}")
    result = subprocess.run([sys.executable, script])

    if result.returncode != 0:
        print(f"Error al ejecutar {script}")
        sys.exit(result.returncode)

print("\nPipeline ejecutado correctamente.")
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
    resultado = subprocess.run([sys.executable, script])

    if resultado.returncode != 0:
        print(f"Error al ejecutar {script}")
        sys.exit(resultado.returncode)

print("\nPipeline ejecutado correctamente.")
# Pipeline DataOps para predicción de churn en telecomunicaciones

## Descripción

Este proyecto implementa un pipeline DataOps para el caso **Telco Customer Churn**, con el objetivo de automatizar el flujo de datos desde la ingesta del dataset hasta su carga en una base de datos relacional PostgreSQL.

El pipeline fue desarrollado en Python y considera cuatro etapas principales:

- Ingesta de datos
- Limpieza y transformación
- Validación estructural y semántica
- Carga a base de datos

Además, el proyecto incluye generación de logs, reportes de validación, evidencia de ejecución, archivo `Dockerfile` y repositorio GitHub como soporte técnico del trabajo.

## Objetivo del proyecto

Desarrollar un pipeline automatizado y reproducible que permita procesar un dataset de churn de clientes de telecomunicaciones, asegurando calidad de datos, trazabilidad del flujo y persistencia en una base de datos PostgreSQL.

## Caso de uso

El proyecto utiliza el dataset **Telco Customer Churn**, que contiene información de clientes, servicios contratados, cargos mensuales, cargos totales y la variable objetivo `Churn`, la cual indica si el cliente abandonó o no la compañía.

## Estructura del proyecto

```text
telco_churn_dataops/
├── data/
│   ├── source/
│   │   └── 02_Base_WA_Fn-UseC_-Telco-Customer-Churn.csv
│   ├── raw/
│   │   └── telco_customer_churn.csv
│   ├── processed/
│   │   └── telco_customer_churn_clean.csv
│   ├── reports/
│   │   └── validation_report.csv
│   ├── validated/
│   │   └── telco_customer_churn_validated.csv
│   ├── inserted/
│   │   └── inserted_records.csv
│   └── rejected/
│       └── rejected_records.csv
├── img/
│   ├── Estructura_proyecto_VSCode.png
│   ├── Ingesta_terminal.png
│   ├── Limpieza_transformacion_terminal.png
│   ├── Validacion_datos_terminal.png
│   ├── Carga_bd_terminal.png
│   ├── validation_report.png
│   ├── pgadmin_tabla.png
│   ├── postgres_count.png
│   └── postgres_rows.png
├── logs/
│   ├── ingesta.log
│   ├── limpieza_transformacion.log
│   ├── validacion_datos.log
│   └── carga_bd.log
├── scripts/
│   ├── ingesta.py
│   ├── limpieza_transformacion.py
│   ├── validacion_datos.py
│   └── carga_bd.py
├── .env.example
├── .gitignore
├── .dockerignore
├── Dockerfile
├── README.md
└── requirements.txt
```

## Tecnologías utilizadas

Python 3
pandas
psycopg2-binary
python-dotenv
PostgreSQL
pgAdmin 4
Git y GitHub
Docker

## Requisitos
Para ejecutar este proyecto se requiere:

Python 3 instalado
pip
PostgreSQL instalado y en ejecución
pgAdmin 4
Git
Docker, de forma opcional, como evidencia técnica

## Instalación de dependencias

Ejecutar en la raíz del proyecto:

pip install -r requirements.txt

### Contenido de requirements.txt:

pandas
psycopg2-binary
python-dotenv

## Variables de entorno

El proyecto utiliza un archivo .env para almacenar las credenciales de conexión a PostgreSQL.

Se incluye un archivo .env.example como plantilla:

PGHOST=localhost
PGPORT=5432
PGDATABASE=telco_churn_db
PGUSER=postgres
PGPASSWORD=tu_contraseña_aqui
Configuración
Copiar .env.example
Renombrarlo a .env
Reemplazar tu_contraseña_aqui por la contraseña real de PostgreSQL

El archivo .env no se sube al repositorio por razones de seguridad.

## Base de datos utilizada

Se utilizó PostgreSQL como motor de base de datos relacional.

### Base creada
telco_churn_db

### Tabla utilizada
telco_churn_cases

La tabla fue diseñada con customerID como clave primaria para asegurar unicidad de registros.

### Etapas del pipeline
1. Ingesta de datos

El script scripts/ingesta.py toma el archivo original ubicado en data/source/, verifica su existencia y que no esté vacío, y genera una copia estructurada en data/raw/telco_customer_churn.csv.

Ejecución

python scripts/ingesta.py

Salida generada
data/raw/telco_customer_churn.csv
logs/ingesta.log

2. Limpieza y transformación

El script scripts/limpieza_transformacion.py realiza una inspección inicial del dataset, revisando:

columnas
tipos de datos
valores faltantes
duplicados
duplicidad de customerID

Luego aplica las transformaciones necesarias:

eliminación de espacios en columnas de texto
conversión de TotalCharges a tipo numérico
imputación de valores inconsistentes de TotalCharges con la mediana
transformación de SeniorCitizen desde 0/1 a No/Si
validación de rangos lógicos:
tenure >= 0
MonthlyCharges >= 0
TotalCharges >= 0
creación de la variable derivada Tenure_Group

Ejecución
python scripts/limpieza_transformacion.py

Salida generada
data/processed/telco_customer_churn_clean.csv
logs/limpieza_transformacion.log

3. Validación estructural y semántica

El script scripts/validacion_datos.py valida el dataset procesado verificando:

Validación estructural
presencia de columnas obligatorias
unicidad de customerID
ausencia de valores nulos en campos críticos
coherencia de tipos y rangos básicos
Validación semántica
valores permitidos en variables categóricas
coherencia entre tenure y Tenure_Group

Ejecución
python scripts/validacion_datos.py

Salida generada
data/reports/validation_report.csv
data/validated/telco_customer_churn_validated.csv
logs/validacion_datos.log

4. Carga a base de datos

El script scripts/carga_bd.py carga el dataset validado a PostgreSQL.

Durante la carga se considera:

lectura del dataset validado
conexión a PostgreSQL mediante variables de entorno
creación de la tabla si no existe
validación previa de cada registro
inserción controlada
manejo de conflictos con clave primaria mediante:
ON CONFLICT (customerID) DO NOTHING

Esto permite evitar duplicados sin borrar datos previamente existentes en la tabla.

Ejecución
python scripts/carga_bd.py

Salida generada
data/inserted/inserted_records.csv
data/rejected/rejected_records.csv
logs/carga_bd.log

### Consultas útiles en PostgreSQL

Ver cantidad total de registros
SELECT COUNT(*) FROM public.telco_churn_cases;

Ver 10 registros cargados
SELECT * FROM public.telco_churn_cases LIMIT 10;

Vaciar la tabla para una nueva demo
TRUNCATE TABLE public.telco_churn_cases;

### Logs de ejecución

El proyecto genera logs por etapa:

logs/ingesta.log
logs/limpieza_transformacion.log
logs/validacion_datos.log
logs/carga_bd.log

Estos archivos permiten dejar trazabilidad de la ejecución y sirven como evidencia técnica del pipeline.

## Evidencias del proyecto

En la carpeta img/ se incluyen capturas de evidencia de:

estructura del proyecto
ejecución de ingesta
ejecución de limpieza y transformación
ejecución de validación
reporte de validación
ejecución de carga a PostgreSQL
tabla creada en pgAdmin
consulta SQL de conteo
consulta SQL de visualización de filas
Dockerfile del proyecto
repositorio GitHub
Dockerfile

## El proyecto incluye un archivo Dockerfile como evidencia de contenerización del entorno de ejecución.

Este Dockerfile permite:

usar una imagen base de Python
instalar dependencias automáticamente
copiar la estructura del proyecto al contenedor
ejecutar un script por defecto
Contenido del Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "scripts/ingesta.py"]
.dockerignore

Se incluye además .dockerignore para evitar copiar al contenedor:

.env
caché de Python
configuración local del editor
historial Git
logs y salidas previas del pipeline
Ejecución resumida del pipeline

### Para correr manualmente las etapas del pipeline:

python scripts/ingesta.py
python scripts/limpieza_transformacion.py
python scripts/validacion_datos.py
python scripts/carga_bd.py
Repositorio GitHub

### El proyecto se encuentra versionado en GitHub e incluye:

scripts del pipeline
logs
evidencias
Dockerfile
archivo .env.example
documentación básica
Consideraciones de seguridad
El archivo .env no se sube al repositorio
Se utiliza .env.example como plantilla pública
PostgreSQL se conecta mediante variables de entorno
customerID se define como clave primaria
La carga controla conflictos sin duplicar registros
Resultados obtenidos

### El pipeline logró:

automatizar las cuatro etapas del flujo de datos
generar trazabilidad mediante logs
validar la consistencia del dataset
cargar exitosamente los registros en PostgreSQL
mantener una estructura organizada y reproducible
incorporar evidencia técnica mediante GitHub, capturas y Dockerfile

## Conclusión

El proyecto permitió implementar un pipeline DataOps funcional para el caso Telco Customer Churn, integrando procesamiento automatizado, validación de calidad de datos y persistencia en PostgreSQL. La solución desarrollada facilita la trazabilidad del flujo, mejora la organización técnica del proyecto y deja una base sólida para futuras tareas de análisis o modelamiento de churn.
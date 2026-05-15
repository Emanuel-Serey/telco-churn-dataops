# Pipeline DataOps para predicción de churn en telecomunicaciones

## Descripción

Este proyecto implementa un pipeline DataOps para el caso **Telco Customer Churn**, con el objetivo de automatizar el flujo de datos desde la ingesta del dataset hasta su carga en una base de datos relacional PostgreSQL.

El pipeline fue desarrollado en Python y considera cuatro etapas principales:

- Ingesta de datos
- Limpieza y transformación
- Validación estructural y semántica
- Carga a base de datos

Además, el proyecto incluye generación de logs, reportes de validación, evidencia de ejecución, archivo `Dockerfile`, archivo `docker-compose.yml` y repositorio GitHub como soporte técnico del trabajo.

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
│   ├── rejected/
│   │   └── rejected_records.csv
│   └── skipped/
│       └── skipped_records.csv
├── img/
├── logs/
│   ├── ingesta.log
│   ├── limpieza_transformacion.log
│   ├── validacion_datos.log
│   └── carga_bd.log
├── scripts/
│   ├── ingesta.py
│   ├── limpieza_transformacion.py
│   ├── validacion_datos.py
│   ├── carga_bd.py
│   └── run_pipeline.py
├── .env.example
├── .gitignore
├── .dockerignore
├── docker-compose.yml
├── Dockerfile
├── README.md
└── requirements.txt
```

## Tecnologías utilizadas

- Python 3
- pandas
- psycopg2-binary
- python-dotenv
- PostgreSQL
- pgAdmin 4
- Git y GitHub
- Docker
- Docker Compose

## Requisitos

Para ejecutar este proyecto se requiere:

- Python 3 instalado
- pip
- PostgreSQL instalado y en ejecución, si se desea trabajar fuera de Docker
- pgAdmin 4, opcional para visualización de la base
- Git
- Docker Desktop
- Docker Compose

## Instalación de dependencias

Ejecutar en la raíz del proyecto:

```bash
pip install -r requirements.txt
```

Contenido de `requirements.txt`:

```txt
pandas
psycopg2-binary
python-dotenv
```

## Variables de entorno

El proyecto utiliza un archivo `.env` para almacenar la configuración de conexión a PostgreSQL.

Se incluye un archivo `.env.example` como plantilla:

```env
PGHOST=db
PGPORT=5432
PGDATABASE=telco_churn_db
PGUSER=postgres
PGPASSWORD=postgres123
```

### Configuración

1. Copiar `.env.example`
2. Renombrarlo a `.env`
3. Ajustar los valores según el entorno de ejecución

> El archivo `.env` no se sube al repositorio por razones de seguridad.

En el entorno Docker Compose, el host de la base corresponde al servicio **db**, por lo que no se utiliza `localhost`, sino el nombre interno del servicio.

## Base de datos utilizada

Se utilizó PostgreSQL como motor de base de datos relacional.

### Base creada

```text
telco_churn_db
```

### Tabla utilizada

```text
telco_churn_cases
```

La tabla fue diseñada con `customerID` como clave primaria para asegurar unicidad de registros.

## Etapas del pipeline

### 1. Ingesta de datos

El script `scripts/ingesta.py` toma el archivo original ubicado en `data/source/`, verifica su existencia y que no esté vacío, y genera una copia estructurada en `data/raw/telco_customer_churn.csv`.

#### Ejecución

```bash
python scripts/ingesta.py
```

#### Salida generada

```text
data/raw/telco_customer_churn.csv
logs/ingesta.log
```

### 2. Limpieza y transformación

El script `scripts/limpieza_transformacion.py` realiza una inspección inicial del dataset, revisando:

- columnas
- tipos de datos
- valores faltantes
- duplicados
- duplicidad de `customerID`

Luego aplica las transformaciones necesarias:

- eliminación de espacios en columnas de texto
- conversión de `TotalCharges` a tipo numérico
- imputación de valores inconsistentes de `TotalCharges` con la mediana
- transformación de `SeniorCitizen` desde `0/1` a `No/Si`
- validación de rangos lógicos:
  - `tenure >= 0`
  - `MonthlyCharges >= 0`
  - `TotalCharges >= 0`
- creación de la variable derivada `Tenure_Group`

#### Ejecución

```bash
python scripts/limpieza_transformacion.py
```

#### Salida generada

```text
data/processed/telco_customer_churn_clean.csv
logs/limpieza_transformacion.log
```

### 3. Validación estructural y semántica

El script `scripts/validacion_datos.py` valida el dataset procesado verificando:

#### Validación estructural

- presencia de columnas obligatorias
- unicidad de `customerID`
- ausencia de valores nulos en campos críticos
- coherencia de tipos y rangos básicos

#### Validación semántica

- valores permitidos en variables categóricas
- coherencia entre `tenure` y `Tenure_Group`

#### Ejecución

```bash
python scripts/validacion_datos.py
```

#### Salida generada

```text
data/reports/validation_report.csv
data/validated/telco_customer_churn_validated.csv
logs/validacion_datos.log
```

### 4. Carga a base de datos

El script `scripts/carga_bd.py` carga el dataset validado a PostgreSQL.

Durante la carga se considera:

- lectura del dataset validado
- conexión a PostgreSQL mediante variables de entorno
- creación de la tabla si no existe
- validación previa de cada registro
- inserción controlada
- manejo de conflictos con clave primaria mediante:

```sql
ON CONFLICT (customerID) DO NOTHING
```

Esto permite evitar duplicados sin borrar datos previamente existentes en la tabla.

Como resultado de la carga, el proyecto genera tres salidas:

- `data/inserted/inserted_records.csv`
- `data/rejected/rejected_records.csv`
- `data/skipped/skipped_records.csv`

El archivo `skipped_records.csv` contiene los registros omitidos por conflicto de clave primaria durante reejecuciones del pipeline.

#### Ejecución

```bash
python scripts/carga_bd.py
```

#### Salida generada

```text
data/inserted/inserted_records.csv
data/rejected/rejected_records.csv
data/skipped/skipped_records.csv
logs/carga_bd.log
```

## Docker y ejecución integrada del pipeline

El proyecto incluye un archivo `Dockerfile` para construir la imagen de la aplicación Python y un archivo `docker-compose.yml` para levantar de forma integrada la aplicación y la base de datos PostgreSQL.

En este esquema se definen dos servicios principales:

- **app**: ejecuta el pipeline completo en Python
- **db**: levanta PostgreSQL como motor de base de datos

El pipeline completo se ejecuta mediante el script `scripts/run_pipeline.py`, que corre secuencialmente las cuatro etapas:

- ingesta
- limpieza y transformación
- validación estructural y semántica
- carga a base de datos

Además, en `docker-compose.yml` se define un volumen para PostgreSQL, permitiendo mantener la persistencia de la base de datos en el mismo equipo aunque los contenedores se detengan o se eliminen.

### Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "scripts/run_pipeline.py"]
```

### docker-compose.yml

```yaml
services:
  app:
    build: .
    container_name: telco_app
    depends_on:
      - db
    environment:
      PGHOST: db
      PGPORT: 5432
      PGDATABASE: telco_churn_db
      PGUSER: postgres
      PGPASSWORD: postgres123
    volumes:
      - .:/app

  db:
    image: postgres:16
    container_name: telco_db
    environment:
      POSTGRES_DB: telco_churn_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres123
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

### Ejecución con Docker Compose

```bash
docker compose up --build
```

Este comando construye la imagen de la aplicación, levanta PostgreSQL y ejecuta automáticamente las cuatro etapas del pipeline.

## Comportamiento ante reejecución del pipeline

Si el pipeline se vuelve a ejecutar, los archivos intermedios del flujo se generan nuevamente en la misma ruta, por lo que se sobreescriben. Esto aplica a los archivos de las carpetas `raw`, `processed`, `validated`, `reports`, `inserted`, `rejected` y `skipped`.

En la etapa de carga, la base de datos no duplica registros ya existentes, ya que `customerID` fue definido como clave primaria y la inserción utiliza la cláusula `ON CONFLICT (customerID) DO NOTHING`. Por ello, en una reejecución los registros previamente cargados no vuelven a insertarse y quedan registrados en `data/skipped/skipped_records.csv` como omitidos por conflicto.

La persistencia de PostgreSQL se mantiene mediante un volumen definido en `docker-compose.yml`, lo que permite conservar la base de datos en el mismo equipo aunque los contenedores se detengan o se eliminen con `docker compose down`.

## Consultas útiles en PostgreSQL

### Ver cantidad total de registros

```sql
SELECT COUNT(*) FROM public.telco_churn_cases;
```

### Ver 10 registros cargados

```sql
SELECT * FROM public.telco_churn_cases LIMIT 10;
```

### Vaciar la tabla para una nueva demo

```sql
TRUNCATE TABLE public.telco_churn_cases;
```

### Consultar la base levantada en Docker Compose

```bash
docker compose exec db psql -U postgres -d telco_churn_db
```

Una vez dentro de `psql`, se pueden ejecutar consultas como:

```sql
SELECT COUNT(*) FROM telco_churn_cases;
SELECT * FROM telco_churn_cases LIMIT 10;
```

Para salir:

```sql
\q
```

## Logs de ejecución

El proyecto genera logs por etapa:

- `logs/ingesta.log`
- `logs/limpieza_transformacion.log`
- `logs/validacion_datos.log`
- `logs/carga_bd.log`

Estos archivos permiten dejar trazabilidad de la ejecución y sirven como evidencia técnica del pipeline.

## Evidencias del proyecto

En la carpeta `img/` se incluyen capturas de evidencia de:

- estructura del proyecto
- ejecución de ingesta
- ejecución de limpieza y transformación
- ejecución de validación
- ejecución de carga a PostgreSQL
- reporte de validación
- logs de cada etapa
- ejecución integrada mediante Docker Compose
- tabla creada en PostgreSQL
- consulta SQL de conteo
- consulta SQL de visualización de filas
- Dockerfile del proyecto
- docker-compose.yml
- repositorio GitHub

## .dockerignore

Se incluye además `.dockerignore` para evitar copiar al contenedor:

- `.env`
- caché de Python
- configuración local del editor
- historial Git
- logs y salidas previas del pipeline

## Ejecución resumida del pipeline

### Ejecución manual por etapas

```bash
python scripts/ingesta.py
python scripts/limpieza_transformacion.py
python scripts/validacion_datos.py
python scripts/carga_bd.py
```

### Ejecución completa del pipeline

```bash
python scripts/run_pipeline.py
```

### Ejecución integrada con Docker Compose

```bash
docker compose up --build
```

## Repositorio GitHub

El proyecto se encuentra versionado en GitHub e incluye:

- scripts del pipeline
- logs
- evidencias
- Dockerfile
- docker-compose.yml
- archivo `.env.example`
- documentación básica

## Consideraciones de seguridad

- El archivo `.env` no se sube al repositorio
- Se utiliza `.env.example` como plantilla pública
- PostgreSQL se conecta mediante variables de entorno
- `customerID` se define como clave primaria
- La carga controla conflictos sin duplicar registros
- Docker Compose separa la aplicación y la base de datos en servicios distintos
- La persistencia local de PostgreSQL se mantiene mediante un volumen

## Resultados obtenidos

El pipeline logró:

- automatizar las cuatro etapas del flujo de datos
- generar trazabilidad mediante logs
- validar la consistencia del dataset
- cargar exitosamente los registros en PostgreSQL
- registrar insertados, rechazados y omitidos por conflicto
- mantener una estructura organizada y reproducible
- incorporar evidencia técnica mediante GitHub, Docker, Docker Compose y capturas

## Conclusión

El proyecto permitió implementar un pipeline DataOps funcional para el caso Telco Customer Churn, integrando procesamiento automatizado, validación de calidad de datos y persistencia en PostgreSQL. La solución desarrollada facilita la trazabilidad del flujo, mejora la organización técnica del proyecto y deja una base sólida para futuras tareas de análisis, modelamiento de churn y despliegue de servicios asociados.

# dask_Ejercicio

Laboratorio de **Computación Distribuida con Dask, Docker & Prefect**: procesamiento
out-of-core de 300.000 registros sucios (mojibake UTF-8, nulos, regex caótico) sobre un
clúster Docker de 1 scheduler + 3 workers, orquestado con Prefect.

## Arquitectura

- **`dask-scheduler`**: coordinador del DAG, expone el puerto `8786` (IPC) y `8787` (Dashboard).
- **`dask-worker-1/2/3`**: workers independientes (2 threads, 1.5 GB RAM c/u), solo accesibles
  en la red interna `dask-cluster-net`.
- **`shared-data/`**: volumen montado en todos los contenedores, actúa como Data Lake local
  (`raw/` para los CSV crudos, `processed/` para el Parquet final).

## Requisitos

- Docker y Docker Compose.

## Uso

1. Construir y levantar el clúster:

```bash
docker compose up -d --build
```

2. Verificar que el Dashboard de Dask esté disponible en http://localhost:8787 y que
   aparezcan los 3 workers conectados.

3. Generar el dataset sintético de 300.000 filas sucias (particionado en 6 CSV):

```bash
docker compose exec dask-scheduler python generate_dirty_data.py
```

4. Ejecutar el flujo orquestado con Prefect (valida infraestructura, corre el DAG de
   limpieza distribuida en el clúster y aplica quality gates sobre el resultado):

```bash
docker compose exec dask-scheduler python prefect_flow.py
```

5. El resultado limpio queda en `shared-data/processed/transactions_clean.parquet`, con las
   columnas `customer_code_clean`, `city_notes_clean` y `phone_clean`.

6. Detener el clúster:

```bash
docker compose down
```

### Probar tolerancia a fallos

Con el flujo en ejecución, en otra terminal:

```bash
docker stop dask-worker-2
```

Observa en el Dashboard cómo el scheduler reprograma las tareas del worker caído hacia
`dask-worker-1` y `dask-worker-3` (ver [ANALISIS.md](ANALISIS.md), pregunta 2).

## Archivos

| Archivo | Propósito |
|---|---|
| [`dockerfile`](dockerfile) | Imagen base compartida por scheduler y workers |
| [`docker-compose.yml`](docker-compose.yml) | Topología del clúster (red, volumen, límites de recursos) |
| [`requirements.txt`](requirements.txt) | Dependencias Python (Dask, Prefect, pandas, pyarrow) |
| [`generate_dirty_data.py`](generate_dirty_data.py) | Generador de 300.000 filas sucias particionadas en 6 CSV |
| [`cleaning_pipeline.py`](cleaning_pipeline.py) | DAG de limpieza distribuida (regex, mojibake, teléfonos) sobre Dask |
| [`prefect_flow.py`](prefect_flow.py) | Orquestación (`@flow`/`@task`), reintentos y quality gates |
| [`ANALISIS.md`](ANALISIS.md) | Respuestas a las preguntas de reflexión arquitectónica |

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

## Rúbrica de Evaluación

Mapeo de cada criterio del taller a su evidencia concreta en este repositorio:

| Criterio | Ponderación | Evidencia en este repo |
|---|---|---|
| **Infraestructura Docker y Clúster** | 20% | [`docker-compose.yml`](docker-compose.yml): 1 scheduler + 3 workers independientes, red `dask-cluster-net`, volumen `shared-data` compartido, límites de recursos (`cpus`/`mem_limit`) por contenedor. Capturas: [dashboard con 3 workers](#capturas-de-pantalla--evidencia). |
| **Script Generador y Datos Sucios** | 20% | [`generate_dirty_data.py`](generate_dirty_data.py): 300.000 filas en 6 CSV, con mojibake, nulos y heterogeneidad regex inyectados probabilísticamente. |
| **Refactorización y Limpieza Dask** | 35% | [`cleaning_pipeline.py`](cleaning_pipeline.py): extracción regex a `CUST-XXXXX`/token de anomalía, reparación de mojibake UTF-8↔Latin-1, normalización de teléfonos, todo vía `map_partitions` distribuido y escrito a Parquet. |
| **Orquestación Prefect y Análisis** | 25% | [`prefect_flow.py`](prefect_flow.py): `@flow`/`@task` con reintentos, validación de infraestructura/datos y `quality_gate` con aserciones; [`ANALISIS.md`](ANALISIS.md): respuestas fundamentadas a las 4 preguntas arquitectónicas. |

## Capturas de Pantalla / Evidencia

Las capturas del Dashboard de Dask (http://localhost:8787) van en [`docs/screenshots/`](docs/screenshots/).
Sugerido, como mínimo:

1. **`01-cluster-status.png`** — pestaña `/status` con el scheduler y los 3 workers conectados (recursos, memoria) recién levantado el clúster (paso 2 del [Uso](#uso)).
2. **`02-task-stream.png`** — pestaña `/status` (Task Stream + Progress) capturada **mientras** corre `prefect_flow.py`, mostrando las tareas distribuidas entre los 3 workers.
3. **`03-worker-failure.png`** — pestaña `/status` justo después de `docker stop dask-worker-2`, mostrando solo 2 workers activos y las tareas reprogramadas (sección [Probar tolerancia a fallos](#probar-tolerancia-a-fallos)).
4. **`04-prefect-flow-run.png`** (opcional) — salida de consola o UI de Prefect mostrando el flujo `dask-cleaning-pipeline` completado con sus tasks (`validate_infrastructure`, `validate_raw_data`, `run_cleaning_dag`, `quality_gate`).

Para incluirlas en este README, guarda el archivo en `docs/screenshots/` y agrega una línea así (ajusta el nombre de archivo y el texto alternativo):

```markdown
![Dashboard de Dask con 3 workers conectados](docs/screenshots/01-cluster-status.png)
```

> Tip: en Windows, `Win + Shift + S` abre el recorte de pantalla; guarda el PNG directamente en
> `docs\screenshots\` dentro de la carpeta del repo.

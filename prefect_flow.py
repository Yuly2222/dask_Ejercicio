#!/usr/bin/env python3
"""
prefect_flow.py
Orquestación del pipeline distribuido con Prefect: valida infraestructura,
verifica datos crudos, somete la limpieza de cada partición como una tarea
independiente (fan-out concurrente, visible como varios 'hilos' de ejecución
paralelos en el grafo del flow run) y aplica quality gates sobre el resultado
final consolidado.
"""
import os
import shutil
from pathlib import Path

import dask.dataframe as dd
from dask.distributed import Client
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner

from cleaning_pipeline import submit_partition_cleaning, ANOMALY_TOKEN

RAW_DIR = os.environ.get("RAW_DIR", "shared-data/raw")
PROCESSED_DIR = os.environ.get("PROCESSED_DIR", "shared-data/processed")
SCHEDULER_ADDRESS = os.environ.get("DASK_SCHEDULER_ADDRESS", "tcp://dask-scheduler:8786")
EXPECTED_ROWS = 1_000_000
OUTPUT_DATASET_DIR = os.path.join(PROCESSED_DIR, "transactions_clean.parquet")


@task(retries=3, retry_delay_seconds=5)
def validate_infrastructure() -> int:
    """Verifica que el scheduler esté vivo y que existan workers conectados
    antes de someter cualquier trabajo al clúster."""
    logger = get_run_logger()
    client = Client(SCHEDULER_ADDRESS, timeout="10s")
    try:
        info = client.scheduler_info()
        n_workers = len(info.get("workers", {}))
        logger.info(f"Scheduler activo en {SCHEDULER_ADDRESS} con {n_workers} worker(s) conectados.")
        if n_workers < 1:
            raise RuntimeError("No hay workers Dask conectados al scheduler.")
        return n_workers
    finally:
        client.close()


@task
def validate_raw_data() -> list[str]:
    """Verifica que los archivos crudos particionados existan antes de lanzar
    el DAG de limpieza."""
    logger = get_run_logger()
    files = sorted(str(p) for p in Path(RAW_DIR).glob("transactions_dirty_part_*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No se encontraron archivos crudos en '{RAW_DIR}'. "
            "Ejecuta primero generate_dirty_data.py."
        )
    logger.info(f"Encontrados {len(files)} archivos crudos en '{RAW_DIR}'.")
    return files


@task
def reset_output_dataset() -> str:
    """Limpia el directorio Parquet de salida antes del fan-out concurrente,
    para que cada partición escriba su propio archivo sin colisiones con
    corridas anteriores."""
    logger = get_run_logger()
    if os.path.isdir(OUTPUT_DATASET_DIR):
        shutil.rmtree(OUTPUT_DATASET_DIR)
    os.makedirs(OUTPUT_DATASET_DIR, exist_ok=True)
    logger.info(f"Directorio de salida reiniciado: {OUTPUT_DATASET_DIR}")
    return OUTPUT_DATASET_DIR


@task(retries=2, retry_delay_seconds=10, name="clean_partition")
def clean_partition_task(raw_file: str, part_index: int) -> str:
    """Limpia UNA partición de forma independiente. `main_flow` somete una
    instancia de esta task por cada archivo crudo vía `.submit()`, lo que hace
    que Prefect las ejecute en paralelo (varios 'hilos' concurrentes visibles
    en el grafo del flow run), mientras cada una delega su cómputo pesado a un
    worker del clúster Dask."""
    logger = get_run_logger()
    part_path = submit_partition_cleaning(raw_file, OUTPUT_DATASET_DIR, SCHEDULER_ADDRESS, part_index)
    logger.info(f"Partición {part_index} ({raw_file}) limpiada -> {part_path}")
    return part_path


@task
def quality_gate(output_dataset_dir: str) -> dict:
    """Aplica aserciones de calidad sobre el resultado final consolidado
    (todas las particiones ya escritas): cardinalidad esperada y proporción
    de anomalías bajo control."""
    logger = get_run_logger()
    # Cliente explícito: al correr esta task en su propio hilo (ConcurrentTaskRunner),
    # no hay un Client Dask "ambient" activo en ese hilo para el .compute().
    with Client(SCHEDULER_ADDRESS) as client:
        ddf = dd.read_parquet(output_dataset_dir)
        total = len(ddf)
        anomalies = int((ddf["customer_code_clean"] == ANOMALY_TOKEN).sum().compute())
        null_notes = int(ddf["city_notes_clean"].isna().sum().compute())
        null_phones = int(ddf["phone_clean"].isna().sum().compute())
    anomaly_ratio = anomalies / total if total else 0.0

    metrics = {
        "total_filas": total,
        "anomalias_customer_code": anomalies,
        "anomaly_ratio": round(anomaly_ratio, 4),
        "notas_nulas": null_notes,
        "telefonos_nulos": null_phones,
    }
    logger.info(f"[QUALITY GATE] {metrics}")

    assert total == EXPECTED_ROWS, f"Se esperaban {EXPECTED_ROWS:,} filas, se obtuvieron {total:,}"
    assert anomaly_ratio < 0.20, f"Proporción de anomalías fuera de rango: {anomaly_ratio:.2%}"

    return metrics


@flow(name="dask-cleaning-pipeline", task_runner=ConcurrentTaskRunner())
def main_flow() -> dict:
    n_workers = validate_infrastructure()
    raw_files = validate_raw_data()
    reset_output_dataset()

    # Fan-out: una task por partición, sometidas concurrentemente. En la UI de
    # Prefect (http://localhost:4200) el grafo del flow run muestra estas 6
    # ramas paralelas entre validate_raw_data y quality_gate.
    futures = [
        clean_partition_task.submit(raw_file, idx)
        for idx, raw_file in enumerate(raw_files)
    ]
    part_paths = [future.result() for future in futures]

    metrics = quality_gate(OUTPUT_DATASET_DIR)
    metrics["workers_conectados"] = n_workers
    metrics["particiones_procesadas"] = len(part_paths)
    return metrics


if __name__ == "__main__":
    result = main_flow()
    print(f"[OK] Flujo Prefect completado: {result}")

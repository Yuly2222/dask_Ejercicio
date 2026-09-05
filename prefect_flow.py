#!/usr/bin/env python3
"""
prefect_flow.py
Orquestación del pipeline distribuido con Prefect: valida infraestructura,
verifica datos crudos, ejecuta el DAG de limpieza sobre el clúster Dask y
aplica quality gates sobre el resultado final.
"""
import os
from pathlib import Path

import dask.dataframe as dd
from dask.distributed import Client
from prefect import flow, task, get_run_logger

from cleaning_pipeline import run_pipeline, ANOMALY_TOKEN

RAW_DIR = os.environ.get("RAW_DIR", "shared-data/raw")
PROCESSED_DIR = os.environ.get("PROCESSED_DIR", "shared-data/processed")
SCHEDULER_ADDRESS = os.environ.get("DASK_SCHEDULER_ADDRESS", "tcp://dask-scheduler:8786")
EXPECTED_ROWS = 300_000


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


@task(retries=2, retry_delay_seconds=10)
def run_cleaning_dag(raw_files: list[str]) -> str:
    """Somete el DAG de limpieza y refactorización al clúster Dask."""
    logger = get_run_logger()
    output_path = run_pipeline(raw_files, PROCESSED_DIR, SCHEDULER_ADDRESS)
    logger.info(f"DAG de limpieza completado. Resultado en: {output_path}")
    return output_path


@task
def quality_gate(output_path: str) -> dict:
    """Aplica aserciones de calidad sobre el resultado final antes de darlo
    por válido: cardinalidad esperada y proporción de anomalías bajo control."""
    logger = get_run_logger()
    ddf = dd.read_parquet(output_path)
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


@flow(name="dask-cleaning-pipeline")
def main_flow() -> dict:
    n_workers = validate_infrastructure()
    raw_files = validate_raw_data()
    output_path = run_cleaning_dag(raw_files)
    metrics = quality_gate(output_path)
    metrics["workers_conectados"] = n_workers
    return metrics


if __name__ == "__main__":
    result = main_flow()
    print(f"[OK] Flujo Prefect completado: {result}")

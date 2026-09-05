#!/usr/bin/env python3
"""
cleaning_pipeline.py
Pipeline distribuido de limpieza y refactorización sobre el dataset de 1.000.000
de filas sucias, ejecutado sobre el clúster Dask (1 scheduler + 3 workers).

Refactoriza:
  - raw_customer_code       -> customer_code_clean ('CUST-XXXXX' o token de anomalía)
  - city_notes_corrupted    -> city_notes_clean (mojibake UTF-8/Latin-1 reparado)
  - phone_raw               -> phone_clean ('+57XXXXXXXXXX' o None si es inválido)
"""
import os
import re
import time

import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client

# Delay artificial por partición (solo con fines didácticos): sin él, la
# limpieza de 1M de filas termina en segundos y no alcanza a observarse en el
# Dask Dashboard (http://localhost:8787). Se compone de un costo fijo por
# partición + un costo proporcional a su tamaño, simulando un procesamiento
# más pesado. Ajustable vía variables de entorno sin tocar código.
PARTITION_BASE_DELAY_SECONDS = float(os.environ.get("PARTITION_BASE_DELAY_SECONDS", "3"))
PARTITION_ROW_DELAY_SECONDS = float(os.environ.get("PARTITION_ROW_DELAY_SECONDS", "0.00004"))

# Patrón regex indicado en la guía: prefijo opcional + 5 dígitos base.
CUSTOMER_CODE_REGEX = re.compile(r"(?:CLI-|cli_|RAW#|CUST-)?(\d{5})", re.IGNORECASE)
ANOMALY_TOKEN = "CUST-00000-ANOMALY"

# Sustituciones de respaldo para secuencias mojibake que no sobreviven un
# roundtrip limpio latin-1 -> utf-8 (p.ej. cuando el extractor de origen ya
# perdió el byte de continuación original).
MOJIBAKE_FALLBACKS = {
    "Ã¡": "á", "Ã©": "é", "Ã­": "í", "Ã-": "í", "Ã³": "ó", "Ãº": "ú",
    "Ã±": "ñ", "Ã‘": "Ñ", "Ã¼": "ü", "Â¿": "¿", "Â¡": "¡",
}

INVALID_PHONE_TOKENS = {"DESCONOCIDO", "N/A", "--", ""}
EXTENSION_PATTERN = re.compile(r"(?i)ext\.?\s*\d+")
NON_DIGIT_PATTERN = re.compile(r"\D")

READ_CSV_DTYPES = {
    "transaction_id": "string",
    "raw_customer_code": "string",
    "city_notes_corrupted": "string",
    "phone_raw": "string",
    "business_category": "string",
}
RAW_TEXT_COLUMNS = ["raw_customer_code", "city_notes_corrupted", "phone_raw"]


def _is_missing(value) -> bool:
    return value is None or value is pd.NA or (isinstance(value, float) and pd.isna(value))


def extract_customer_code(raw_value) -> str:
    """Aplica la extracción regex y refactoriza a 'CUST-XXXXX', o imputa el
    token de anomalía si la fila es nula/vacía o no contiene un código válido."""
    if _is_missing(raw_value):
        return ANOMALY_TOKEN
    text = str(raw_value).strip()
    if not text:
        return ANOMALY_TOKEN
    match = CUSTOMER_CODE_REGEX.search(text)
    if not match:
        return ANOMALY_TOKEN
    return f"CUST-{match.group(1)}"


def fix_mojibake(text) -> str:
    """Repara texto corrompido por una decodificación cruzada UTF-8 <-> Latin-1,
    con un mapa de respaldo para secuencias que no admiten un roundtrip limpio."""
    if _is_missing(text):
        return text
    if not isinstance(text, str):
        return text
    fixed = text
    try:
        fixed = fixed.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    for broken, correct in MOJIBAKE_FALLBACKS.items():
        fixed = fixed.replace(broken, correct)
    return fixed.strip()


def clean_phone(raw_value):
    """Normaliza teléfonos caóticos a formato '+57XXXXXXXXXX', o None si el
    valor es un placeholder de inválido o no tiene 10 dígitos reconocibles."""
    if _is_missing(raw_value):
        return None
    text = str(raw_value).strip()
    if not text or text.upper() in INVALID_PHONE_TOKENS:
        return None
    text = EXTENSION_PATTERN.sub("", text)
    digits = NON_DIGIT_PATTERN.sub("", text)
    if digits.startswith("0057"):
        digits = digits[4:]
    elif digits.startswith("057"):
        digits = digits[3:]
    elif digits.startswith("57") and len(digits) > 10:
        digits = digits[2:]
    if len(digits) != 10:
        return None
    return f"+57{digits}"


def _clean_partition(partition: pd.DataFrame) -> pd.DataFrame:
    """Función aplicada por bloque vía map_partitions: cada worker ejecuta
    esta lógica sobre su partición en memoria acotada, sin serializar filas
    individuales de vuelta al scheduler."""
    partition = partition.copy()
    partition["customer_code_clean"] = partition["raw_customer_code"].apply(extract_customer_code).astype("string")
    partition["city_notes_clean"] = partition["city_notes_corrupted"].apply(fix_mojibake).astype("string")
    partition["phone_clean"] = partition["phone_raw"].apply(clean_phone).astype("string")

    delay = PARTITION_BASE_DELAY_SECONDS + len(partition) * PARTITION_ROW_DELAY_SECONDS
    time.sleep(delay)

    return partition


def _build_meta() -> pd.DataFrame:
    return pd.DataFrame({
        "transaction_id": pd.Series(dtype="string"),
        "raw_customer_code": pd.Series(dtype="string"),
        "city_notes_corrupted": pd.Series(dtype="string"),
        "phone_raw": pd.Series(dtype="string"),
        "amount_usd": pd.Series(dtype="float64"),
        "business_category": pd.Series(dtype="string"),
        "customer_code_clean": pd.Series(dtype="string"),
        "city_notes_clean": pd.Series(dtype="string"),
        "phone_clean": pd.Series(dtype="string"),
    })


def run_pipeline(raw_files, output_dir: str, scheduler_address: str) -> str:
    """Conecta al clúster Dask, ejecuta la limpieza distribuida sobre TODOS los
    archivos crudos a la vez (un único DAG) y escribe el resultado consolidado
    en Parquet. Uso directo por CLI (ver __main__ más abajo)."""
    client = Client(scheduler_address)
    try:
        print(f"[*] Conectado al clúster Dask: {client}")
        # dtype="string" (nullable) en vez de "object": evita un bug de
        # inferencia de esquema de PyArrow en map_partitions/to_parquet con
        # columnas de texto genéricas en esta versión de Dask.
        ddf = dd.read_csv(raw_files, dtype=READ_CSV_DTYPES)
        cleaned = ddf.map_partitions(_clean_partition, meta=_build_meta())
        cleaned = cleaned.drop(columns=RAW_TEXT_COLUMNS)

        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "transactions_clean.parquet")
        cleaned.to_parquet(output_path, engine="pyarrow", write_index=False, overwrite=True)
        print(f"[OK] Pipeline distribuido completado -> {output_path}")
        return output_path
    finally:
        client.close()


def clean_and_write_partition(raw_file: str, output_dataset_dir: str, part_index: int) -> str:
    """Limpia UN único archivo crudo y escribe su propio archivo Parquet
    independiente (sin depender de las demás particiones). Diseñada para
    ejecutarse como tarea remota en un worker Dask vía `client.submit()`,
    sometida por separado para cada partición desde Prefect: al lanzar varias
    de estas concurrentemente (`.submit()` por archivo), Prefect muestra
    varios 'hilos' de ejecución paralelos en el grafo del flow run, además del
    paralelismo real que ya ocurre entre los workers de Dask."""
    df = pd.read_csv(raw_file, dtype=READ_CSV_DTYPES)
    df = _clean_partition(df)
    df = df.drop(columns=RAW_TEXT_COLUMNS)

    os.makedirs(output_dataset_dir, exist_ok=True)
    part_path = os.path.join(output_dataset_dir, f"part-{part_index:02d}.parquet")
    df.to_parquet(part_path, engine="pyarrow", index=False)
    return part_path


def submit_partition_cleaning(raw_file: str, output_dataset_dir: str, scheduler_address: str, part_index: int) -> str:
    """Conecta al clúster Dask y somete la limpieza de una única partición
    como una tarea remota independiente (`client.submit`). Cada llamada a esta
    función corre en su propio hilo de Prefect (ver `clean_partition_task` en
    prefect_flow.py), permitiendo el fan-out concurrente visible en su UI."""
    client = Client(scheduler_address)
    try:
        future = client.submit(clean_and_write_partition, raw_file, output_dataset_dir, part_index, pure=False)
        return future.result()
    finally:
        client.close()


if __name__ == "__main__":
    import sys
    from pathlib import Path

    raw_dir = sys.argv[1] if len(sys.argv) > 1 else "shared-data/raw"
    files = sorted(str(p) for p in Path(raw_dir).glob("transactions_dirty_part_*.csv"))
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos crudos en {raw_dir}")
    run_pipeline(files, "shared-data/processed", os.environ.get("DASK_SCHEDULER_ADDRESS", "tcp://dask-scheduler:8786"))

# Análisis y Reflexión Arquitectónica

Respuestas a las preguntas de diseño de sistemas distribuidos de la Guía de Laboratorio
(Computación Distribuida con Dask, Docker & Prefect).

## 1. Cuellos de Botella de Red vs. CPU

La transferencia de datos entre contenedores introduce un costo que compite directamente con
el tiempo de CPU dedicado al parsing de regex y a la reparación de encoding. En este taller,
cada partición CSV (~50.000 filas) se lee **localmente por cada worker** desde el volumen
compartido `shared-data` (montado en todos los contenedores), por lo que el I/O pesado no viaja
por la red del clúster: solo viajan las tareas del grafo, sus metadatos y, cuando es necesario,
resultados intermedios entre workers (shuffles).

El punto de inflexión donde la arquitectura distribuida deja de ser ventajosa frente a un
proceso mononodo ocurre cuando:
- El tamaño de cada partición es pequeño en relación al overhead de serialización
  (pickle/cloudpickle) y al costo fijo de programar una tarea en el scheduler (~1 ms por tarea).
- Las transformaciones son puramente vectorizadas y ya caben en memoria de un solo nodo — en
  ese caso, Pandas puro sería más rápido porque evita la coordinación del DAG.
- Hay una operación con "shuffle" (join, groupby con muchas claves, sort global) que obliga a
  mover particiones completas entre workers por la red Docker (`dask-cluster-net`), lo cual sí
  es sensible al ancho de banda de la red virtual bridge.

En este pipeline, como la limpieza es "embarazosamente paralela" (cada fila se procesa de forma
independiente vía `map_partitions`, sin joins ni shuffles), el cuello de botella dominante es la
CPU (regex + funciones Python por fila), no la red. La red solo se usa para coordinación
(scheduler ↔ workers) y para escribir el Parquet final, que es liviano por partición.

## 2. Tolerancia a Fallos y Recomputación

Al ejecutar `docker stop dask-worker-2` durante el procesamiento:

1. El **scheduler** detecta la pérdida del heartbeat del worker (timeout configurable) y lo
   marca como caído, removiéndolo del pool de workers activos.
2. Todas las tareas que estaban **en ejecución** en ese worker, y cualquier resultado
   intermedio que solo existiera en su memoria (sin réplica), se marcan como perdidos.
3. Dask reconstruye esas tareas a partir del **grafo de dependencias (DAG)**: como cada tarea
   conoce sus inputs (las particiones CSV originales en `shared-data`, que persisten en disco),
   el scheduler simplemente **reprograma** las tareas huérfanas hacia los workers
   supervivientes (`dask-worker-1` y `dask-worker-3`), sin intervención manual.
4. El costo de la recomputación es proporcional al trabajo que se había hecho en ese worker
   (no se repite todo el DAG, solo la porción afectada), gracias a que Dask mantiene el estado
   de qué tareas ya completaron y persistieron su resultado.

Esto ilustra la ventaja del patrón Master-Worker con un DAG explícito: la resiliencia no
depende de que el worker sobreviva, sino de que el trabajo sea **determinista y
reproducible** a partir de sus dependencias, que es justamente la razón por la que los datos
crudos se leen desde el volumen compartido en lugar de mantenerse solo en memoria.

## 3. Ventajas de Parquet sobre CSV

El pipeline exporta a Apache Parquet en lugar de concatenar un CSV de 300k filas por varias
razones arquitectónicas:

- **Compresión columnar**: Parquet almacena los datos por columna (no por fila), lo que permite
  aplicar codecs de compresión (snappy, zstd) mucho más efectivos porque los valores de una
  misma columna comparten tipo y distribución (p. ej. `business_category` con solo 5 valores
  distintos comprime casi a costo cero comparado con texto plano repetido en CSV).
- **Filtrado por predicados (predicate pushdown)**: al leer Parquet, un motor de consulta puede
  usar los metadatos de cada row-group (min/max por columna) para **saltarse bloques enteros**
  sin descomprimirlos ni leerlos, si el filtro no puede cumplirse en ese rango (p. ej.
  `amount_usd > 1000` puede omitir row-groups completos). CSV no tiene metadatos ni orden
  columnar, por lo que siempre exige un escaneo completo fila por fila.
- **Tipado y esquema explícito**: Parquet conserva el tipo de cada columna (string, float,
  nulls) sin ambigüedad de parsing, evitando errores de inferencia de tipo que sí ocurren al
  releer un CSV (p. ej. una columna que mezcla números y texto).
- **Lectura paralela nativa**: Parquet está particionado internamente en row-groups, lo que
  permite a Dask leer y procesar múltiples row-groups en paralelo entre workers sin tener que
  particionar manualmente un archivo monolítico como se requeriría con un CSV único.
- **Tamaño en disco**: para datasets con alta cardinalidad de texto repetido (como
  `business_category` o `customer_code_clean`), Parquet con *dictionary encoding* reduce
  drásticamente el tamaño frente a CSV en texto plano.

## 4. Separación de Responsabilidades (Prefect vs. Dask)

Dask resuelve el problema de **cómputo distribuido**: construir y ejecutar un DAG de tareas
sobre datos particionados, gestionando memoria, paralelismo y recomputación ante fallos de
workers. Sin embargo, Dask no sabe nada sobre el **ciclo de vida de negocio** del pipeline:

- **Gobernanza y visibilidad**: Prefect expone el estado de cada paso (`Scheduled`, `Running`,
  `Completed`, `Failed`) en una UI/API independiente del clúster de cómputo, permitiendo que un
  equipo de operaciones (no necesariamente los mismos ingenieros de datos) monitoree el pipeline
  sin conocer los detalles internos del DAG de Dask.
- **Reintentos con semántica de negocio**: Prefect permite declarar políticas de reintento
  (`retries`, `retry_delay_seconds`) sobre pasos completos del flujo (p. ej. "si falla la
  conexión al scheduler, reintenta 3 veces cada 5s") de forma independiente a la tolerancia a
  fallos interna de Dask sobre tareas individuales. Son dos capas distintas: Dask reintenta
  *tareas* dentro de un cómputo ya sometido; Prefect reintenta *pasos* del flujo completo
  (incluyendo, por ejemplo, la validación de infraestructura antes de siquiera someter trabajo).
- **Quality gates y aserciones**: Prefect permite insertar tareas de validación (`quality_gate`)
  como ciudadanos de primera clase del flujo, con logging y trazabilidad, en lugar de
  aserciones sueltas dentro de un script.
- **Composición y reutilización**: un `@flow` de Prefect puede orquestar múltiples DAGs de Dask
  (o incluso pipelines que no usan Dask en absoluto) como pasos de un proceso de negocio más
  amplio, con parámetros, programación (cron) y notificaciones — capacidades que Dask no
  provee porque no es su responsabilidad.

En resumen: Dask es el motor de **ejecución paralela de bajo nivel**; Prefect es la capa de
**orquestación y gobernanza** que decide cuándo, cómo y bajo qué condiciones ese motor se
invoca, y qué hacer cuando algo falla a nivel de proceso de negocio.

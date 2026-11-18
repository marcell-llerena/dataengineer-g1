# Fundamentos de AWS para Data Engineering

```
fundamentos-aws/
├── notebooks/            01 S3 · 02 Glue Crawler · 03 Athena · 04 Glue Job (boto3)
├── glue_jobs/            script PySpark del Glue Job (ventas_diarias.py)
├── dags/                 DAG de Airflow que orquesta todo, un día a la vez
├── data/                 staging/ y reportes/ que dejó el ETL local
├── credentials/          .csv de la access key (no se sube a Git)
├── Dockerfile            imagen de Airflow + provider de Amazon
├── docker-compose.yaml   Airflow local (LocalExecutor)
└── .env.example          bucket y Connection de AWS
```

Los notebooks construyen el data lake **a mano**, paso a paso. El DAG hace **lo mismo, automático y para cada día**.

---

## Airflow + AWS: el DAG `ventas_diarias_aws`

Airflow corre en tu máquina; los datos y el cómputo están en AWS. Airflow solo **orquesta**: decide qué se ejecuta, en qué orden y qué hacer si algo falla.

```
verificar_recursos ─┬─> preparar_catalogo ─┐
                    ├─> subir_ventas ──────┴─> registrar_particion_ventas ─┐
                    └─> subir_script ──────────────────────────────────────┴─> ejecutar_glue_job
                                                                                     │
        validar_resultado <── registrar_particion_diarias <── esperar_salida <──────┘
```

| Tarea | Qué hace | Cómo | En los notebooks |
|---|---|---|---|
| `verificar_recursos` | ¿Con quién trabajo? ¿Existen el bucket y el rol? | `AwsBaseHook`, `S3Hook` | 01 (ej. 0) |
| `preparar_catalogo` | Crea la base y las tablas si no existen | `AthenaHook` | 02 (crawler) |
| `subir_ventas` | `staging/<día>/ventas.csv` → Parquet → `processed/ventas/year=/month=/day=/` | `S3Hook` | 01 (ej. 6) |
| `subir_script` | `glue_jobs/ventas_diarias.py` → `s3://<bucket>/scripts/` | `S3Hook` | 04 (ej. 3) |
| `registrar_particion_ventas` | `ALTER TABLE ventas ADD IF NOT EXISTS PARTITION ...` | `AthenaOperator` | 02 (días nuevos) |
| `ejecutar_glue_job` | Lanza `ventas-diarias` con `--FECHA=<día>` y espera a que termine | `GlueJobOperator` | 04 (ej. 4–5) |
| `esperar_salida` | Comprueba que el job escribió Parquet en `curated/ventas_diarias/` | `S3KeySensor` | 04 (ej. 6) |
| `registrar_particion_diarias` | Registra el día en la tabla `ventas_diarias` | `AthenaOperator` | 04 (ej. 8) |
| `validar_resultado` | Consulta Athena y compara con `data/reportes/reporte_<día>.csv` | `AthenaHook` | 03 + 04 (ej. 6) |

### Hook, Operator y Sensor

| | Qué es | Cuándo usarlo | Ejemplo en el DAG |
|---|---|---|---|
| **Hook** | Cliente de un servicio con las credenciales de una Connection. `hook.conn` es el cliente de boto3 | Cuando necesitas **tu propia lógica** (convertir a Parquet, comparar resultados) | `S3Hook(...).load_file(...)` dentro de un `@task` |
| **Operator** | Una tarea ya armada: solo le pasas parámetros | Cuando la tarea es **estándar** (ejecutar SQL, lanzar un job) | `AthenaOperator(query=...)` |
| **Sensor** | Un operator que **espera** a que algo ocurra | Archivos que llegan, procesos externos | `S3KeySensor(bucket_key=...)` |

Todos usan la misma Connection: **`aws_curso`**. Ninguna credencial aparece en el código.

---

## Requisitos

1. **Usuario IAM** con las políticas de `playground/fundamentos_aws/README.md` (S3, Glue, Athena e IAM).
2. **El bucket**: notebook 01, ejercicio 1.
3. **El rol `AWSGlueServiceRole-curso`**: notebook 02, ejercicio 1 (o por consola, paso 4 del README de IAM).
4. **Docker Desktop**.

No hace falta haber ejecutado el resto: si las tablas o el job `ventas-diarias` no existen, el DAG los crea; si ya existen, los reutiliza.

> Si ejecutaste la limpieza de los notebooks (`BORRAR = True`), vuelve a correr el ejercicio 1 del notebook 01 y el ejercicio 1 del notebook 02.

---

## 1. Credenciales: la Connection `aws_curso`

Airflow lee las Connections de variables de entorno con el formato `AIRFLOW_CONN_<ID>`. La nuestra está en `.env`:

```bash
cp .env.example .env
```

Edita `.env`:

- `AWS_BUCKET`: tu bucket.
- `AIRFLOW_UID`: en Linux, el resultado de `id -u`.
- `AIRFLOW_CONN_AWS_CURSO`: elige **una** opción.

**Opción A (recomendada): perfil `curso`.** docker-compose monta tu `~/.aws` en el contenedor, en solo lectura. Si aún no tienes el perfil:

```bash
aws configure --profile curso
#   AWS Access Key ID:     (del .csv de credentials/)
#   AWS Secret Access Key: (del .csv de credentials/)
#   Default region name:   us-east-2
#   Default output format: json
```

**Opción B: access key en `.env`.** Si no tienes la CLI, comenta la línea de la opción A y descomenta la de la opción B, con los valores del `.csv`.

> ⚠️ `.env` **no se sube a Git** y está excluido de la imagen de Docker (`.dockerignore`). Las credenciales nunca van en el código ni en el Dockerfile.

---

## 2. Levantar Airflow

```bash
docker compose up -d --build
```

- UI: http://localhost:8082 (usuario `airflow`, clave `airflow`).
- ¿El DAG cargó sin errores?

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

---

## 3. Probar un solo día

Antes de activar el DAG, ejecuta **un día** de punta a punta desde la terminal. Tarda unos 3–4 minutos (casi todo es el arranque del cluster de Glue) y cuesta ≈ USD 0.03:

```bash
docker compose exec airflow-scheduler airflow dags test ventas_diarias_aws 2026-10-22
```

Al final, `validar_resultado` imprime la comparación entre la nube y el reporte local y termina con `OK: Glue y el ETL local coinciden ...`.

---

## 4. Activar el DAG: los 8 días

En la UI, activa (*unpause*) `ventas_diarias_aws`. Como tiene `catchup=True`, `start_date=2026-10-20` y `end_date=2026-10-27`, Airflow crea **una ejecución por día** y las procesa en orden, una a la vez (`max_active_runs=1`).

- Tiempo: unos 30 minutos.
- Costo: ≈ USD 0.25 (8 ejecuciones del Glue Job). Las consultas de Athena escanean KB y las DDL no se cobran.

Mientras corre, mira en la UI la vista **Graph** y los **logs** de cada tarea.

### Resultado en Athena

```sql
SELECT year, month, day, COUNT(*) AS filas, ROUND(SUM(monto_total), 2) AS monto
FROM ventas_db.ventas_diarias
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
```

### ¿Y si lo ejecuto dos veces?

El resultado es el mismo: el DAG es **idempotente**.

- `load_file(..., replace=True)` sobrescribe el Parquet del día.
- `ADD IF NOT EXISTS PARTITION` no duplica la partición.
- El Glue Job usa `partitionOverwriteMode = dynamic`: reemplaza solo su día.

---

## Si algo falla

Abre el log de la tarea en rojo y busca el mensaje completo.

| Tarea | Mensaje | Causa y solución |
|---|---|---|
| `verificar_recursos` | `ProfileNotFound` / `Unable to locate credentials` | La Connection no encuentra credenciales: revisa el `.env` (opción A u opción B) y reinicia con `docker compose up -d` |
| `verificar_recursos` | `Define AWS_BUCKET ...` / `No existe el bucket ...` / `No existe el rol ...` | Completa `.env` o ejecuta el notebook indicado |
| cualquiera | `explicit deny in a service control policy` | Región incorrecta: la Connection debe tener `"region_name": "us-east-2"` |
| `ejecutar_glue_job` | `FAILED` | Detalle en CloudWatch Logs, grupo `/aws-glue/jobs/error`. `AccessDenied` en S3 → al rol le falta la política inline (notebook 02) |
| `esperar_salida` | timeout | El job terminó pero no escribió la partición: revisa el log del job |
| `validar_resultado` | `no coincide con el reporte local` | El log muestra la tabla con la columna `ok`: compara fila a fila |

Más diagnóstico de permisos en `playground/fundamentos_aws/README.md` → *¿Por qué me da AccessDenied?*

---

## Apagar y limpiar

```bash
docker compose down -v   # apaga Airflow y borra su base de metadatos (no toca AWS)
```

Los recursos de AWS se borran con las celdas de limpieza de los notebooks, en orden inverso: **04 → 03 → 02 → 01**. El DAG usa los mismos nombres (`ventas-diarias`, `ventas_db`, `curated/ventas_diarias/`), así que esas celdas también borran lo que creó Airflow.

# Kafka + Spark Structured Streaming (simulado)

Pipeline de streaming en local: un productor genera transacciones falsas, Kafka las transporta y Spark Structured Streaming las valida y las procesa.

> Kafka es la tubería, Spark es la fábrica.

## Arquitectura

```
producer.py  ->  Kafka (Redpanda)  ->  Spark Structured Streaming  ->  data/valid/       (Parquet)
 eventos JSON     topic            validación + ventanas             data/quarantine/  (inválidos)
 simulados      "transacciones"
```

## Estado

- [x] Productor de eventos simulados (con un % de eventos defectuosos a propósito)
- [x] `docker-compose.yml` con Redpanda
- [x] Consumidor Spark: validación y separación válidos / cuarentena (lógica probada con eventos de ejemplo)
- [x] Métricas por ventanas de 1 minuto y tienda (por consola)
- [ ] Prueba de extremo a extremo con Kafka real
- [ ] Diagrama y capturas

## Estructura

```
kafka-spark-streaming/
├── producer/producer.py   # genera transacciones y las envía a Kafka
├── spark/                 # consumidor con Spark Structured Streaming
├── data/
│   ├── valid/             # eventos válidos (Parquet)
│   └── quarantine/        # eventos que no pasan la validación
├── docker-compose.yml     # broker Kafka-compatible (Redpanda)
└── requirements.txt
```

## Evento de ejemplo

```json
{
  "id": "fd4e60d5-761f-42ae-ac38-f8678498a90d",
  "timestamp": "2026-10-23T17:21:26.277435+00:00",
  "tienda": "Hospitalet",
  "pais": "IT",
  "categoria": "electronica",
  "metodo_pago": "tarjeta",
  "importe": 264.72
}
```

Defectos que introduce el productor: importe negativo, importe no numérico, tienda vacía y país inválido.

## Cómo ejecutarlo

```bash
pip install -r requirements.txt

# 1) Probar el productor sin Kafka
python producer/producer.py --dry-run --max 10

# 2) Levantar el broker
docker compose up -d

# 3) En una terminal: lanzar el job de Spark (necesita Java 17 o 21)
python spark/streaming_job.py

# 4) En otra terminal: enviar eventos al topic
python producer/producer.py --rate 5 --bad-ratio 0.1
```

El job escribe los eventos válidos en `data/valid/` (Parquet) y los rechazados en `data/quarantine/` (JSON con el evento original y el motivo: `importe_no_positivo`, `importe_no_numerico`, `tienda_vacia`, `pais_invalido`, `timestamp_invalido`, `json_invalido`). Cada 10 s imprime cuántos eventos fueron a cada sitio y las métricas por ventana.

### Nota para Windows

Spark necesita `winutils.exe` y `hadoop.dll` para funcionar en Windows. Colócalos en `C:\hadoop\bin` y, antes de lanzar el job, define en PowerShell:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH += ";C:\hadoop\bin"
```

Sin esto falla con `HADOOP_HOME and hadoop.home.dir are unset`. En Linux, macOS o WSL no hace falta.

Opciones del job: `--bootstrap`, `--topic`, `--out-valid`, `--out-quarantine`, `--checkpoint`, `--starting-offsets` (`latest`/`earliest`), `--trigger` (segundos entre micro-batches).

Opciones del productor: `--servidor`, `--topic`, `--rate` (eventos/s), `--bad-ratio` (0-1), `--max`, `--dry-run`.

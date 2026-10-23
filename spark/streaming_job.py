"""
Consumidor con Spark Structured Streaming.

Lee el topic de Kafka, valida cada evento y lo separa en:
  - válidos     -> data/valid/       (Parquet)
  - inválidos   -> data/quarantine/  (JSON, con el evento original y el motivo)
Además calcula métricas por ventana de 1 minuto y tienda (se muestran por consola).

Uso:
    python spark/streaming_job.py
    python spark/streaming_job.py --starting-offsets earliest --trigger 5
"""

import argparse

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

PAISES_VALIDOS = ["ES", "FR", "PT", "IT"]

# Todo como string: así un importe "N/A" no rompe el parseo y lo validamos nosotros.
SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("timestamp", T.StringType()),
        T.StructField("tienda", T.StringType()),
        T.StructField("pais", T.StringType()),
        T.StructField("categoria", T.StringType()),
        T.StructField("metodo_pago", T.StringType()),
        T.StructField("importe", T.StringType()),
    ]
)


def parse_and_validate(raw: DataFrame) -> DataFrame:
    """
    Recibe un DataFrame con una columna `value` (el JSON del evento, como string o bytes)
    y devuelve las columnas del evento ya tipadas, más:
      - raw:    el JSON original
      - motivo: null si el evento es válido, o el motivo del rechazo
    """
    df = raw.select(F.col("value").cast("string").alias("raw"))
    df = df.withColumn("e", F.from_json("raw", SCHEMA))

    importe = F.col("e.importe").cast("double")
    ts = F.to_timestamp(F.col("e.timestamp"))

    motivo = (
        # Con JSON roto, from_json devuelve un struct con todos los campos a null
        F.when(
            F.col("e").isNull()
            | (F.col("e.id").isNull() & F.col("e.timestamp").isNull() & F.col("e.importe").isNull()),
            F.lit("json_invalido"),
        )
        .when(F.col("e.id").isNull(), F.lit("id_vacio"))
        .when(F.col("e.tienda").isNull() | (F.trim(F.col("e.tienda")) == ""), F.lit("tienda_vacia"))
        .when(importe.isNull(), F.lit("importe_no_numerico"))
        .when(importe <= 0, F.lit("importe_no_positivo"))
        .when(~F.col("e.pais").isin(PAISES_VALIDOS) | F.col("e.pais").isNull(), F.lit("pais_invalido"))
        .when(ts.isNull(), F.lit("timestamp_invalido"))
    )

    return df.select(
        "raw",
        F.col("e.id").alias("id"),
        ts.alias("timestamp"),
        F.col("e.tienda").alias("tienda"),
        F.col("e.pais").alias("pais"),
        F.col("e.categoria").alias("categoria"),
        F.col("e.metodo_pago").alias("metodo_pago"),
        importe.alias("importe"),
        motivo.alias("motivo"),
    )


def split_valid_invalid(validated: DataFrame):
    valid = validated.filter(F.col("motivo").isNull()).drop("raw", "motivo")
    invalid = validated.filter(F.col("motivo").isNotNull()).select("raw", "motivo")
    return valid, invalid


def crear_spark() -> SparkSession:
    # El conector de Kafka debe coincidir con la versión de PySpark instalada.
    paquete = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
    spark = (
        SparkSession.builder.appName("kafka-spark-streaming")
        .master("local[*]")
        .config("spark.jars.packages", paquete)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    p = argparse.ArgumentParser(description="Spark Structured Streaming sobre Kafka")
    p.add_argument("--bootstrap", default="localhost:9092", help="Broker de Kafka")
    p.add_argument("--topic", default="transacciones")
    p.add_argument("--out-valid", default="data/valid")
    p.add_argument("--out-quarantine", default="data/quarantine")
    p.add_argument("--checkpoint", default="checkpoints")
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    p.add_argument("--trigger", type=int, default=10, help="Segundos entre micro-batches")
    args = p.parse_args()

    spark = crear_spark()

    def leer_kafka() -> DataFrame:
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .load()
        )

    # --- Query 1: separar válidos / cuarentena ---
    def procesar_lote(batch_df: DataFrame, batch_id: int):
        validated = parse_and_validate(batch_df).persist()
        valid, invalid = split_valid_invalid(validated)
        n_valid, n_invalid = valid.count(), invalid.count()
        if n_valid:
            valid.write.mode("append").parquet(args.out_valid)
        if n_invalid:
            invalid.write.mode("append").json(args.out_quarantine)
        print(f"[batch {batch_id}] válidos={n_valid} cuarentena={n_invalid}")
        validated.unpersist()

    q_split = (
        leer_kafka()
        .writeStream.foreachBatch(procesar_lote)
        .option("checkpointLocation", f"{args.checkpoint}/split")
        .trigger(processingTime=f"{args.trigger} seconds")
        .start()
    )

    # --- Query 2: métricas por ventana de 1 minuto y tienda (solo eventos válidos) ---
    validos_stream = parse_and_validate(leer_kafka()).filter(F.col("motivo").isNull())
    metricas = (
        validos_stream.withWatermark("timestamp", "1 minute")
        .groupBy(F.window("timestamp", "1 minute"), "tienda")
        .agg(
            F.count("*").alias("n_transacciones"),
            F.round(F.sum("importe"), 2).alias("total_importe"),
            F.round(F.avg("importe"), 2).alias("importe_medio"),
        )
    )
    q_metricas = (
        metricas.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", f"{args.checkpoint}/metricas")
        .trigger(processingTime=f"{args.trigger} seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()
    q_split.stop()
    q_metricas.stop()


if __name__ == "__main__":
    main()

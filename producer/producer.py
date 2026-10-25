"""
Productor de transacciones simuladas para Kafka.

Genera eventos JSON falsos y los envía a un topic de Kafka.
Un pequeño porcentaje de eventos sale defectuoso a propósito
(para que Spark los valide y los mande a cuarentena).

Uso:
    python producer.py --dry-run                 # solo imprime, sin Kafka
    python producer.py                           # envía a localhost:9092, topic "transacciones"
    python producer.py --rate 5 --bad-ratio 0.1  # 5 eventos/s, 10% defectuosos
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

TIENDAS = ["Barcelona-Centro", "Hospitalet", "Esplugues", "Madrid-Sol", "Valencia-Puerto"]
PAISES = ["ES", "ES", "ES", "FR", "PT", "IT"]  # ES más frecuente
CATEGORIAS = ["alimentacion", "electronica", "ropa", "hogar", "ocio"]
METODOS_PAGO = ["tarjeta", "efectivo", "bizum"]


def generar_evento() -> dict:
    """Devuelve una transacción válida."""
    return {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tienda": random.choice(TIENDAS),
        "pais": random.choice(PAISES),
        "categoria": random.choice(CATEGORIAS),
        "metodo_pago": random.choice(METODOS_PAGO),
        "importe": round(random.uniform(1, 500), 2),
    }


def estropear_evento(evento: dict) -> dict:
    """Introduce un defecto aleatorio en el evento."""
    defecto = random.choice(["importe_negativo", "sin_tienda", "importe_texto", "pais_raro"])
    if defecto == "importe_negativo":
        evento["importe"] = -abs(evento["importe"])
    elif defecto == "sin_tienda":
        evento["tienda"] = None
    elif defecto == "importe_texto":
        evento["importe"] = "N/A"
    elif defecto == "pais_raro":
        evento["pais"] = "??"
    return evento


def crear_productor(servidor: str):
    # Import aquí para que --dry-run funcione sin tener kafka-python instalado
    from kafka import KafkaProducer
    from kafka.serializer import Serializer

    # kafka-python 3.x exige un objeto Serializer (ya no vale una lambda).
    class SerializadorJson(Serializer):
        def serialize(self, topic, headers, data):
            return json.dumps(data).encode("utf-8")

    return KafkaProducer(
        bootstrap_servers=servidor,
        value_serializer=SerializadorJson(),
    )


def main():
    parser = argparse.ArgumentParser(description="Productor de transacciones simuladas")
    parser.add_argument("--servidor", default="localhost:9092", help="Broker de Kafka")
    parser.add_argument("--topic", default="transacciones", help="Nombre del topic")
    parser.add_argument("--rate", type=float, default=2, help="Eventos por segundo")
    parser.add_argument("--bad-ratio", type=float, default=0.05, help="Proporción de eventos defectuosos (0-1)")
    parser.add_argument("--max", type=int, default=0, help="Nº de eventos a enviar (0 = infinito)")
    parser.add_argument("--dry-run", action="store_true", help="Imprime los eventos sin enviarlos a Kafka")
    args = parser.parse_args()

    productor = None if args.dry_run else crear_productor(args.servidor)
    enviados = 0

    try:
        while args.max == 0 or enviados < args.max:
            evento = generar_evento()
            if random.random() < args.bad_ratio:
                evento = estropear_evento(evento)

            if args.dry_run:
                print(json.dumps(evento, ensure_ascii=False))
            else:
                productor.send(args.topic, evento)

            enviados += 1
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print(f"\nParado. Eventos generados: {enviados}")
    finally:
        if productor:
            productor.flush()
            productor.close()


if __name__ == "__main__":
    main()

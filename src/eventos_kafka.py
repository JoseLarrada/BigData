from kafka import KafkaProducer
from datetime import datetime
import json

TOPIC_DEFAULT = "procesamiento_kpi"

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def enviar_evento(evento: dict, topic: str = TOPIC_DEFAULT):
    evento["timestamp"] = datetime.utcnow().isoformat()
    producer.send(topic, evento)
    producer.flush()


from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer
import json
import asyncio
import threading

app = FastAPI()
websockets = []

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websockets.append(websocket)
    try:
        while True:
            await asyncio.sleep(1)
    except:
        websockets.remove(websocket)

@app.get("/")
def root():
    return {"message": "Servidor FastAPI para predicción de números escritos a mano"}


def consumir_kafka():
    consumer = KafkaConsumer(
        'procesamiento_kpi',
        bootstrap_servers='kafka:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='grupo_web'
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def enviar_a_todos(evento):
        for ws in websockets:
            await ws.send_text(json.dumps(evento))

    for mensaje in consumer:
        evento = mensaje.value
        loop.run_until_complete(enviar_a_todos(evento))

# Lanza el consumidor en un hilo aparte
threading.Thread(target=consumir_kafka, daemon=True).start()

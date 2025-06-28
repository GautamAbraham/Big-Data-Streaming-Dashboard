import asyncio
import configparser
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer
import os

# Load config
config = configparser.ConfigParser()
config.read(os.getenv("CONFIG_FILE", "config.ini"))
KAFKA_TOPIC = config['DEFAULT'].get('KAFKA_TOPIC', 'processed-data-output')
KAFKA_BOOTSTRAP_SERVERS = config['DEFAULT'].get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

app = FastAPI()
clients = set()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    try:
        while True:
            await asyncio.sleep(100)  # Keep connection open
    except WebSocketDisconnect:
        clients.remove(websocket)

async def kafka_to_websocket():
    import time
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: m.decode("utf-8"),
        group_id="websocket-backend",
        auto_offset_reset="latest"  # Only get new messages
    )
    await consumer.start()
    last_sent = time.time()
    try:
        async for msg in consumer:
            data = msg.value
            print("Kafka message received:", data)  # Log received message
            for ws in set(clients):
                try:
                    await ws.send_text(data)
                    last_sent = time.time()
                except Exception:
                    clients.remove(ws)
            # Heartbeat: send a ping every 30 seconds if no data
            if time.time() - last_sent > 30:
                for ws in set(clients):
                    try:
                        await ws.send_text('{"type":"ping"}')
                    except Exception:
                        clients.remove(ws)
                last_sent = time.time()
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(kafka_to_websocket())
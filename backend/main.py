import asyncio
import configparser
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer
from fastapi.middleware.cors import CORSMiddleware 
import json
import logging
from typing import Set
from pydantic import BaseModel
from kafka import KafkaProducer
import atexit
from fastapi import Body
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
config = configparser.ConfigParser()
config.read(os.getenv("CONFIG_FILE", "config.ini"))
KAFKA_TOPIC = config['DEFAULT'].get('KAFKA_TOPIC', 'normal-data')
KAFKA_CRITICAL_TOPIC = config['DEFAULT'].get('KAFKA_CRITICAL_TOPIC', 'critical-data')
KAFKA_BOOTSTRAP_SERVERS = config['DEFAULT'].get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

CONFIG_TOPIC = config['DEFAULT'].get('CONFIG_TOPIC', 'playback-config')   # <-- Add this if not present

producer_config = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "value_serializer": lambda v: json.dumps(v).encode('utf-8'),
}
config_producer = KafkaProducer(**producer_config)
atexit.register(lambda: config_producer.close())

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

clients: Set[WebSocket] = set()
playback_speed = 1.0

class PlaybackSpeedRequest(BaseModel):
    playback_speed: float

@app.post("/api/playback_speed")
async def set_playback_speed(speed_req: PlaybackSpeedRequest):
    global playback_speed
    playback_speed = speed_req.playback_speed
    config_producer.send(CONFIG_TOPIC, {"playback_speed": float(playback_speed)})
    config_producer.flush()
    logger.info(f"[BACKEND] Set playback speed to {playback_speed} (sent to {CONFIG_TOPIC})")
    return {"playback_speed": playback_speed}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    logger.info(f"Client connected. Total clients: {len(clients)}")
    try:
        while True:
            await asyncio.sleep(100)
    except WebSocketDisconnect:
        clients.remove(websocket)
        logger.info(f"Client disconnected. Total clients: {len(clients)}")

async def broadcast_to_clients(message: str):
    """Broadcast message to all connected clients."""
    for ws in clients.copy():
        try:
            await ws.send_text(message)
        except:
            clients.discard(ws)

async def kafka_to_websocket():
    """Simple Kafka consumer with basic retry."""
    while True:
        try:
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC, KAFKA_CRITICAL_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: m.decode("utf-8"),
                group_id="websocket-backend",
                auto_offset_reset="latest"
            )
            
            await consumer.start()
            logger.info(f"Kafka consumer started")
            
            async for msg in consumer:
                try:
                    data = json.loads(msg.value)
                    
                    # Add priority metadata
                    if msg.topic == KAFKA_CRITICAL_TOPIC:
                        data["data_priority"] = "critical"
                        logger.info(f"CRITICAL DATA: {len(clients)} clients")
                    else:
                        data["data_priority"] = "normal"
                    
                    await broadcast_to_clients(json.dumps(data))
                    
                except Exception as e:
                    logger.warning(f"Error processing message: {e}")
                    
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
            await asyncio.sleep(5)  # Simple retry

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(kafka_to_websocket())

@app.get("/health")
async def health_check():
    return {"status": "healthy", "connected_clients": len(clients)}
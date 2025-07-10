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

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
config = configparser.ConfigParser()
config.read(os.getenv("CONFIG_FILE", "config.ini"))
KAFKA_TOPIC = config['DEFAULT'].get('KAFKA_TOPIC', 'processed-data-output')
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
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
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
            await asyncio.sleep(100)  # Keep connection open
    except WebSocketDisconnect:
        clients.remove(websocket)
        logger.info(f"Client disconnected. Total clients: {len(clients)}")

async def broadcast_to_clients(message: str):
    """Broadcast message to all connected clients with error handling."""
    if not clients:
        return
    
    disconnected_clients = set()
    for ws in clients.copy():
        try:
            await ws.send_text(message)
        except Exception as e:
            logger.warning(f"Failed to send to client: {e}\n{traceback.format_exc()}")
            disconnected_clients.add(ws)
    
    # Remove disconnected clients
    clients.difference_update(disconnected_clients)

async def kafka_to_websocket():
    """Enhanced Kafka consumer with better error handling and reconnection for both normal and critical data."""
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        consumer = None
        try:
            # Subscribe to both normal and critical data topics
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                KAFKA_CRITICAL_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: m.decode("utf-8"),
                group_id="websocket-backend",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000
            )
            
            await consumer.start()
            logger.info(f"Kafka consumer started for topics: {KAFKA_TOPIC}, {KAFKA_CRITICAL_TOPIC}")
            retry_count = 0  # Reset retry count on successful connection
            
            last_heartbeat = asyncio.get_event_loop().time()
            
            async for msg in consumer:
                try:
                    data = msg.value
                    topic = msg.topic
                    
                    # Validate JSON
                    parsed_data = json.loads(data)  # This will raise an exception if invalid JSON
                    
                    # Add topic metadata to help frontend distinguish critical vs normal data
                    if topic == KAFKA_CRITICAL_TOPIC:
                        parsed_data["data_priority"] = "critical"
                        parsed_data["source_topic"] = "critical-data"
                        logger.info(f"CRITICAL DATA: Broadcasting critical alert to {len(clients)} clients")
                    else:
                        parsed_data["data_priority"] = "normal"
                        parsed_data["source_topic"] = "processed-data-output"
                        logger.debug(f"Broadcasting normal data to {len(clients)} clients")
                    
                    # Re-serialize with added metadata
                    enriched_data = json.dumps(parsed_data)
                    logger.info(f"SENDING TO WS CLIENTS: {enriched_data}")
                    await broadcast_to_clients(enriched_data)
                    last_heartbeat = asyncio.get_event_loop().time()
                    
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON received: {data}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                
                # Send heartbeat every 30 seconds
                current_time = asyncio.get_event_loop().time()
                if current_time - last_heartbeat > 30:
                    await broadcast_to_clients('{"type":"heartbeat","timestamp":"' + str(current_time) + '"}')
                    last_heartbeat = current_time
                    
        except Exception as e:
            retry_count += 1
            logger.error(f"Kafka consumer error (attempt {retry_count}/{max_retries}): {e}")
            if consumer:
                try:
                    await consumer.stop()
                except:
                    pass
            
            if retry_count < max_retries:
                wait_time = min(2 ** retry_count, 30)  # Exponential backoff, max 30s
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger.error("Max retries reached. Kafka consumer failed.")
                break

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(kafka_to_websocket())

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "connected_clients": len(clients),
        "kafka_topics": {
            "normal_data": KAFKA_TOPIC,
            "critical_data": KAFKA_CRITICAL_TOPIC
        }
    }
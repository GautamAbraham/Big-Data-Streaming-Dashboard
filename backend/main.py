import asyncio
import configparser
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer
import os
import json
import logging
from typing import Set

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
config = configparser.ConfigParser()
config.read(os.getenv("CONFIG_FILE", "config.ini"))
KAFKA_TOPIC = config['DEFAULT'].get('KAFKA_TOPIC', 'processed-data-output')
KAFKA_BOOTSTRAP_SERVERS = config['DEFAULT'].get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

app = FastAPI()
clients: Set[WebSocket] = set()

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
            logger.warning(f"Failed to send to client: {e}")
            disconnected_clients.add(ws)
    
    # Remove disconnected clients
    clients.difference_update(disconnected_clients)

async def kafka_to_websocket():
    """Enhanced Kafka consumer with better error handling and reconnection."""
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        consumer = None
        try:
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: m.decode("utf-8"),
                group_id="websocket-backend",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000
            )
            
            await consumer.start()
            logger.info(f"Kafka consumer started for topic: {KAFKA_TOPIC}")
            retry_count = 0  # Reset retry count on successful connection
            
            last_heartbeat = asyncio.get_event_loop().time()
            
            async for msg in consumer:
                try:
                    data = msg.value
                    # Validate JSON
                    json.loads(data)  # This will raise an exception if invalid JSON
                    
                    logger.debug(f"Broadcasting message to {len(clients)} clients")
                    await broadcast_to_clients(data)
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
        "kafka_topic": KAFKA_TOPIC
    }
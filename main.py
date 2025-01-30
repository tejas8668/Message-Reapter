import asyncio
import os
import logging
from pyrogram import Client, filters
from collections import deque
from typing import Dict, List, Deque

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


api_id = os.environ.get("api_id")
api_hash = os.environ.get("api_hash")
bot_token = os.environ.get("bot_token")

# Channel Config
CHANNELS = {
    "group1": {
        "sources": ["-1002487065354"],
        "destinations": ["-1002464896968"]
    },
    "group2": {
        "sources": ["-1002398034096"],
        "destinations": ["-1002176533426"]
    }
}

# Configurable Parameters
MAX_QUEUE_SIZE = 72  # Maximum messages to store in the queue
FORWARD_COUNT = 5  # Number of messages to forward at a time
REPEAT_TIME = 3600  # Time to wait before forwarding next batch of messages (in seconds)

# Initialize message queues for each source channel
message_queues: Dict[str, Dict[str, Deque[int]]] = {}
for group, config in CHANNELS.items():
    message_queues[group] = {}
    for source in config["sources"]:
        message_queues[group][source] = deque(maxlen=MAX_QUEUE_SIZE)

app = Client("loop_forwarder", api_id, api_hash, bot_token=bot_token)

# Event handler to store messages from source channels
@app.on_message(filters.chat([channel for config in CHANNELS.values() for channel in config["sources"]]))
async def collect_messages(client, message):
    """
    Collects messages from source channels and stores them in the appropriate queue.

    Args:
        client: The Pyrogram client.
        message: The incoming message.
    """
    group = None
    for config in CHANNELS.values():
        if message.chat.id in config["sources"]:
            group = [key for key, value in CHANNELS.items() if value == config][0]
            break

    if group:
        message_queues[group][message.chat.id].append(message.message_id)
        logger.info(f"✅ Message added: {message.message_id} | Queue Size: {len(message_queues[group][message.chat.id])}/72")

# Function to forward messages from the queue
async def forward_messages(group):
    """
    Forwards messages from the queue for the specified group.

    Args:
        group (str): The group name.
    """
    while True:
        for source, queue in message_queues[group].items():
            if not queue:
                logger.info("🚀 Queue is empty! Waiting for new messages...")
                await asyncio.sleep(10)  # 10 sec wait before checking again
                continue

            for _ in range(min(FORWARD_COUNT, len(queue))):
                msg_id = queue.popleft()
                for destination in CHANNELS[group]["destinations"]:
                    try:
                        await app.forward_messages(destination, source, msg_id)
                        logger.info(f"📤 Forwarded: {msg_id} | Queue Size: {len(queue)}/72")
                    except Exception as e:
                        logger.error(f"Failed to forward message {msg_id}: {e}")

        await asyncio.sleep(REPEAT_TIME)  # Wait for REPEAT_TIME before next batch

# Start bot and scheduler
async def main():
    async with app:
        tasks = []
        for group in CHANNELS:
            tasks.append(asyncio.create_task(forward_messages(group)))
        await asyncio.gather(*tasks)
        await app.run()

asyncio.run(main())

import json

from aiokafka import AIOKafkaProducer
import asyncio


async def send_one():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092')
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        for counter in range(10):
            if counter % 2:
                data = {
                    "message": f"Message № {counter}"
                }
                bytes_data = json.dumps(data).encode('utf-8')
            else:
                bytes_data = f"Bla-bla {counter}".encode('utf-8')
            await producer.send_and_wait("research", bytes_data)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

asyncio.run(send_one())

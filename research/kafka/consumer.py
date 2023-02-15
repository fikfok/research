from aiokafka import AIOKafkaConsumer
import asyncio
import json


async def consume():
    consumer = AIOKafkaConsumer(
        'research',
        bootstrap_servers='localhost:9092',
        group_id="my-group")
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            # print("consumed: ", msg.topic, msg.partition, msg.offset,
            #       msg.key, loads(msg.value.decode('utf-8')), msg.timestamp)
            try:
                data = json.loads(msg.value.decode('utf-8'))
            except:
                data = msg.value.decode('utf-8')
            print(data)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()

asyncio.run(consume())

## Kafka Consumer/Producer library for IoT Hub Alpha.

### installation:
`pip install git+https://github.com/IoT-Hub-Alpha/kafka-consumer-producer-lib.git@dev`

or

`git+https://github.com/IoT-Hub-Alpha/kafka-consumer-producer-lib.git@dev`

in `requirements.txt`, make sure docker image has git installed if running from docker.

**@dev indicates the dev branch inside the `https://github.com/IoT-Hub-Alpha/kafka-consumer-producer-lib` repo, if you need a modified version of this library, you can create a new branch, modify it and install that.**



## Usage:
## Producer:
- **Import producer and shared exception handler:**

    `from IoTKafka import IoTKafkaProducer, IoTProducerException`

- **Initialize producer:**

    `producer = IoTKafkaProducer()`

**IoTKafkaProducer accepts `KWARGS` only, acceptable kwargs and thier uses:**

|kwarg|Definition|value|
|---------------|--------------|--------------|
|acks| kafka acknologments | default env |
| linger_ms | kafka producer linger | default env |
| batch_size | kafka producer batchsize | default env |
| compression_type | kafka conpression | default env |
|request_timeout_ms | timeout in ms | default env |

- **Produce to topic:**
```
producer.produce(
    topic="telemetry.clean",
    key="SN-111-VVVV",
    value=payload,
    on_delivery=batch_delivery_callback,
    attempts = 3
)
```
IoT Kafka producer includes retry attempts (default of 3) and can be overwritted with `attempts`, errors during producing will throw a `IoTProducerException` exception with attached reason.

**Note: value and key can convert passed values to bytes automatically, no need to .encode('utf-8'), although passing encoded data works aswell!**

## Consumer

 - **Import consumer**:

    `from IoTKafka import IoTKafkaConsumer`

 - **Initialize consumer**:
    `raw_consumer = IoTKafkaConsumer(group_id="test-clean-consumer", enable_auto_offset_store=True)`

**Consumer accepts KWARGS only, acceptable kwargs and thier uses:**

|kwarg|Definition|value|
|---------------|--------------|--------------|
|group_id| group id of the consumer, **REQUIRED FIELD** | None |
| auto_offset_reset | auto offset kafka reset | default env |
| enable_auto_commit | enables/disabled auto commit on cunsome | default env |
| enable_auto_offset_store | enables offset store | default true |
|max_poll_interval_ms | max poll interval | default env (15min)|

 - **Subscribe to desired topic**:

    `raw_consumer.subscribe(["telemetry.clean"])`

 - **Consume messages as normal**:

```
messages = raw_consumer.consume(
    num_messages=1, timeout=1
    )
```

### Example Sample producer/consumer loop:
```python
import json
from IoTKafka import IoTKafkaProducer, IoTKafkaConsumer
from time import sleep

producer = IoTKafkaProducer()
raw_consumer = IoTKafkaConsumer(group_id="test-clean-consumer", enable_auto_offset_store=True)
raw_consumer.subscribe(["telemetry.clean"])


def batch_delivery_callback(err, msg: str):
    batch_errors = []
    if err:
        batch_errors.append(err)

payload = {"hey": 1, "bye": 2}

def test_consumer():
    messages = raw_consumer.consume(
        num_messages=1, timeout=1
            )
    print(messages)

def test_producer():
    producer.produce(
        topic="telemetry.clean",
        key="SN-111-VVVV",
        value=payload,
        on_delivery=batch_delivery_callback,
    )

while True:
    test_producer()
    test_consumer()
    sleep(2)
```
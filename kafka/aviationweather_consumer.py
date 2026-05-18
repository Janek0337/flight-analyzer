import json

BOOTSTRAP_SERVER = "localhost:9092"
GROUP_ID = "aviationweather-metar-debug"
TOPIC = "weather"


def main():
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVER,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC])
    print("[+] Start consumer")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[-] Error reading {msg.error()}")
                continue

            payload = msg.value().decode("utf-8")
            event = json.loads(payload)
            print(f"[{event['station_id']}] {event['observation_time']} {event['temp_c']}C {event['flight_category']}")
    finally:
        consumer.close()


if __name__ == "__main__":
    raise SystemExit(main())
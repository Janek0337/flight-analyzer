from confluent_kafka import Producer
import uuid
import random
import json

def make_report(err, msg):
    if err:
        print(f'[-] Fail: {err}')
    else:
        print(f'[+] Success: {msg.value().decode('utf-8')}')

# bootstrap server mówi gdzie ma głównego serwera szukać
producer = Producer({'bootstrap.servers': 'localhost:9092'})

json_to_send = {
    'id': str(uuid.uuid4()),
    'bet_value': random.randint(0, 100)
}

# kafka przyjmuje tylko surowe bajty
byte_json = json.dumps(json_to_send).encode('utf-8')

producer.produce(
    topic='bets',
    value=byte_json,
    callback=make_report
)
producer.flush()
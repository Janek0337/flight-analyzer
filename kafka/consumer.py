from confluent_kafka import Consumer
import json

# group id określa id maszyn, które mają te same zreplikowane dane
consumer = Consumer(
    {
    'bootstrap.server': 'localhost:9092',
    'group_id': 'gambling',
    'auto.offset.reset': 'earliest'
    }
)

consumer.subscribe(['bets'])

print('[+] Successful subscribtion')

while True:
    msg = consumer.poll(1.0)

    # jesli nie ma nowych wiadomosci
    if msg is None:
        continue
    if msg.error():
        print('[-] Error reading', msg.error())
        continue

    value = msg.value().decode('utf-8')
    result = json.loads(value)
    print(f'Guy with id {result['id']} gambled ${result['bet_value']}... What a fool haha')
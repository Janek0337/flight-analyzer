import json
from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP = 'localhost:9092'

# Konfiguracja konsumenta
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'airport_specific_model_group', # Zmieniona nazwa grupy
    'auto.offset.reset': 'latest'
}

def run_consumer():
    consumer = Consumer(conf)
    
    # Subskrybujemy tematy, które odpowiadają lotniskom
    target_topics = ["EGLL", "EDDB", "EPWA", "EDDF"]
    consumer.subscribe(target_topics)
    
    print(f"[*] Konsument gotowy. Nasłuchuję na tematach: {target_topics}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[!] Błąd Kafki: {msg.error()}")
                    continue

            # Dekodujemy dane
            airport_topic = msg.topic()
            event_type = msg.key().decode('utf-8') if msg.key() else "UNKNOWN"
            data = json.loads(msg.value().decode('utf-8'))

            # Wyświetlamy w zależności od tego, czy to przylot czy odlot
            if event_type == "arrival":
                print(f"[{airport_topic}] PRZYLOT | Lot: {data.get('flight_number')} z {data.get('origin_iata')} | Status: {data.get('status')}")
            elif event_type == "departure":
                print(f"[{airport_topic}] ODLOT  | Lot: {data.get('flight_number')} do {data.get('dest_iata')} | Status: {data.get('status')}")
            else:
                print(f"[{airport_topic}] INNE   | Typ: {event_type} | Dane: {data}")

    except KeyboardInterrupt:
        print("\n[!] Zamykanie konsumenta...")
    finally:
        # Pamiętaj o czystym zamknięciu!
        consumer.close()

if __name__ == "__main__":
    run_consumer()
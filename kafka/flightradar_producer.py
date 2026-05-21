import json
import time
from confluent_kafka import Producer
from flightradar24API import get_airport_arrivals_departures

KAFKA_BOOTSTRAP = 'localhost:9092'
TARGET_AIRPORTS = ["EGLL", "EDDB", "EPWA", "EDDF"]

# Inicjalizacja potężnego producenta Confluent
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def delivery_report(err, msg):
    """Raportuje status dostarczenia wiadomości do brokera."""
    if err is not None:
        print(f"[!] Błąd dostarczenia wiadomości: {err}")

def run_pipeline():
    print(f"[*] Połączono z Kafką ({KAFKA_BOOTSTRAP}). Ruszamy z pobieraniem...")

    while True:
        try:
            for airport in TARGET_AIRPORTS:
                # W tym podejściu nazwa lotniska jest naszym tematem (topic) w Kafce
                topic_name = airport
                print(f"\n[>] Odpytuję API dla lotniska: {airport} (Temat: {topic_name})")
                
                flight_data = get_airport_arrivals_departures(airport, limit=10, mode="current")
                
                # --- PRZYLOTY (ARRIVALS) ---
                if "arrivals" in flight_data and flight_data["arrivals"]:
                    for arrival in flight_data["arrivals"]:
                        payload = json.dumps(arrival).encode('utf-8')
                        # Kluczem jest teraz sztywny string "arrival"
                        key = b"arrival"
                        
                        producer.produce(
                            topic=topic_name,
                            key=key,
                            value=payload,
                            callback=delivery_report
                        )
                    print(f"    Wysłano {len(flight_data['arrivals'])} przylotów.")

                # --- ODLOTY (DEPARTURES) ---
                if "departures" in flight_data and flight_data["departures"]:
                    for departure in flight_data["departures"]:
                        payload = json.dumps(departure).encode('utf-8')
                        # Kluczem jest teraz sztywny string "departure"
                        key = b"departure"
                        
                        producer.produce(
                            topic=topic_name,
                            key=key,
                            value=payload,
                            callback=delivery_report
                        )
                    print(f"    Wysłano {len(flight_data['departures'])} odlotów.")

            # Flush wymusza fizyczne wypchnięcie danych z bufora do Kafki
            producer.flush()
            
            print("\n[*] Przerwa 60 sekund przed kolejnym strzałem do API...")
            time.sleep(60)

        except KeyboardInterrupt:
            print("\n[!] Zatrzymano skrypt ręcznie.")
            break
        except Exception as e:
            print(f"\n[!] Wystąpił błąd: {e}")
            time.sleep(10)

if __name__ == "__main__":
    run_pipeline()
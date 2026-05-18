# skrypt uruchamiać tylko pierwszy raz na maszynie, chociaż użycie go drugi raz nie spowoduje błędu

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

def init_kafka():
    
    admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

    topics = [
        NewTopic("flights", num_partitions=3, replication_factor=1),
        NewTopic("weather", num_partitions=3, replication_factor=1),
        NewTopic("bets", num_partitions=3, replication_factor=1)
    ]

    results = admin.create_topics(topics)

    for topic_name, result in results.items():
        try:
            result.result() 
            print(f"[+] Topic '{topic_name}' has been created")
            
        except KafkaException as e:
            err = e.args[0]

            if err.code() == 36:
                print(f"[#] Topic '{topic_name}' already exists, skipping")
            else:
                print(f"[-] Error while creating topic '{topic_name}': {err}")

if __name__ == "__main__":
    init_kafka()
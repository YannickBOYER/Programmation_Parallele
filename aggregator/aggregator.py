import pika
import json
import time
from collections import defaultdict
from datetime import datetime

RABBITMQ_HOST = "rabbitmq"
RESULT_QUEUE_NAME = "results"

def wait_for_rabbitmq():
    for _ in range(10):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            print("[Aggrégateur] Connecté à RabbitMQ.")
            return connection
        except:
            print("[Aggrégateur] En attente de RabbitMQ...")
            time.sleep(3)
    raise Exception("[Aggrégateur] Connexion impossible.")

connection = wait_for_rabbitmq()
channel = connection.channel()

# Stockage local en mémoire
resultats = defaultdict(list)

def callback(ch, method, properties, body):
    message = json.loads(body)
    if message["type"] == "END":
        channel.stop_consuming()
        return

    ville = message["ville"]
    type_resultat = message["type"]
    valeur = message["valeur"]

    # Stocke dans une structure mémoire
    resultats[ville].append({
        "type": type_resultat,
        "valeur": valeur,
    })

    print(f"[Aggrégateur] Reçu de {ville} → {type_resultat}: {valeur}")

if __name__ == "__main__":
    channel.queue_declare(queue=RESULT_QUEUE_NAME)  # Queue commune pour tous les résultats

    channel.basic_consume(
        queue=RESULT_QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=True
    )

    print("[Aggrégateur] En attente de messages des workers...")
    channel.start_consuming()
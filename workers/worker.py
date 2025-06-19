import pika
import json
import os
import time
from collections import defaultdict

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
VILLE = os.getenv("VILLE")  # ex: "lyon"
QUEUE_NAME = f"transactions_{VILLE}"
RESULT_QUEUE_NAME = "results"

chiffre_affaires = 0
repartition = defaultdict(int)

def wait_for_rabbitmq():
    for _ in range(10):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            print(f"[Worker-{VILLE}] Connecté à RabbitMQ.")
            return connection
        except:
            print(f"[Worker-{VILLE}] En attente de RabbitMQ...")
            time.sleep(3)
    raise Exception("[Worker] Échec de connexion.")

connection = wait_for_rabbitmq()
channel = connection.channel()

def callback(ch, method, properties, body):
    global chiffre_affaires, repartition
    transaction = json.loads(body)

    # Vérification que la ville correspond bien
    if transaction['ville'].lower() != VILLE:
        return  # Ignore si pas la bonne ville (sécurité)

    prix = transaction['prix']
    chiffre_affaires += prix
    # print(f"[Worker-{VILLE}] {transaction['transaction_id']} → {type_transac} = {prix} €")
    # print(f"[Worker-{VILLE}] Total CA = {chiffre_affaires} €, Répartition = {dict(repartition)}\n")
    publish_result("chiffre_affaires", chiffre_affaires)

    # Répartition des transactions
    type_transac = transaction['type']
    repartition[type_transac] += 1
    publish_result("repartition", dict(repartition))


def publish_result(type, valeur):
    message = {
        "ville": VILLE,
        "type": type,
        "valeur": valeur,
    }
    # Envoi du résultat dans la queue commune
    channel.basic_publish(
        exchange='',
        routing_key=RESULT_QUEUE_NAME,
        body=json.dumps(message)
    )

if __name__ == "__main__":
    if not VILLE:
        raise Exception("VILLE n'est pas définie dans les variables d'environnement.")

    channel.queue_declare(queue=QUEUE_NAME)
    channel.queue_declare(queue=RESULT_QUEUE_NAME)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    print(f"[Worker-{VILLE}] En attente de messages...")
    channel.start_consuming()

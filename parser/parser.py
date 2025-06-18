import csv
from datetime import datetime
import pika
import json
import os
import time

CSV_PATH = os.getenv("CSV_PATH", "./data/transactions_autoconnect.csv")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

def lire_et_envoyer_csv(filepath):
    # Connexion à RabbitMQ
    connection = wait_for_rabbitmq(RABBITMQ_HOST)
    channel = connection.channel()

    # Déclare une queue (idempotent)
    channel.queue_declare(queue='transactions')

    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            try:
                transaction = {
                    'transaction_id': row['transaction_id'],
                    'date': row['date'],
                    'ville': row['ville'].strip(),
                    'type': row['type'].strip().lower(),
                    'modele': row['modele'].strip(),
                    'prix': float(row['prix']),
                    'duree_location_mois': int(row['duree_location_mois']) if row['duree_location_mois'] else None
                }
                print(transaction)
                # Envoi dans RabbitMQ
                channel.basic_publish(
                    exchange='',
                    routing_key='transactions',
                    body=json.dumps(transaction)
                )
                print(f"[Parser] Envoyé : {transaction['transaction_id']}")

            except Exception as e:
                print(f"[Parser] ⚠️ Ligne {i+2} ignorée : {e}")
    connection.close()
    print("[Parser] ✅ Fini d'envoyer les transactions.")

def wait_for_rabbitmq(host, max_retries=10):
    for i in range(max_retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            print("[Parser] ✅ Connexion établie avec RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print(f"[Parser] 🔄 Tentative de connexion à RabbitMQ ({i+1}/{max_retries})...")
            time.sleep(3)
    raise Exception("[Parser] ❌ Impossible de se connecter à RabbitMQ.")

if __name__ == "__main__":
    lire_et_envoyer_csv(CSV_PATH)

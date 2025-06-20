#!/usr/bin/env python3
import pandas as pd
import pika
import json
import os
import time

CSV_PATH       = os.environ.get("CSV_PATH", "./data/transactions_autoconnect.csv")
RABBITMQ_HOST  = os.environ.get("RABBITMQ_HOST", "rabbitmq")
NOMBRE_WORKERS = int(os.environ.get("NUM_WORKERS", "3"))

# Connexion à RabbitMQ
print("[Parser] Connexion à RabbitMQ…")
connection = None
for _ in range(10):
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST)
        )
        break
    except pika.exceptions.AMQPConnectionError:
        print("[Parser] Erreur de connexion, retry dans 10s…")
        time.sleep(10)
if not connection or not connection.is_open:
    raise RuntimeError("[Parser] Impossible de se connecter à RabbitMQ.")

channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='results',     durable=True)
channel.queue_declare(queue='aggregated_results', durable=True)

def lire_csv(chemin):
    """Lit le CSV et renvoie un DataFrame."""
    print(f"[Parser] Chargement des données depuis {chemin}")
    return pd.read_csv(chemin, dtype=str)

def scinder_donnees(df, nb_workers):
    """Divise le DataFrame en chunks pour les workers."""
    taille = len(df) // nb_workers + (1 if len(df) % nb_workers else 0)
    chunks = [df.iloc[i:i+taille] for i in range(0, len(df), taille)]
    print(f"[Parser] Données divisées en {len(chunks)} chunks")
    return chunks

def distribuer_batchs(chunks):
    """Publie chaque chunk sur task_queue."""
    total = len(chunks)
    for i, chunk in enumerate(chunks):
        message = {
            "type":     "batch",
            "batch_id": i+1,
            "data":     chunk.to_dict(orient="records")
        }
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[Parser] Batch {i+1}/{total} publié ({len(chunk)} lignes)")
    return total

def notifier_fin(total_batchs):
    """Publie le signal FIN sur la queue results."""
    message = {
        "type":          "FIN",
        "total_batches": total_batchs
    }
    channel.basic_publish(
        exchange='',
        routing_key='results',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print(f"[Parser] Signal FIN publié dans 'results' (total={total_batchs})")

def main():
    df = lire_csv(CSV_PATH)
    chunks = scinder_donnees(df, NOMBRE_WORKERS)
    total = distribuer_batchs(chunks)
    notifier_fin(total)
    channel.close()
    connection.close()

if __name__ == "__main__":
    main()

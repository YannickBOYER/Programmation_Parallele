#!/usr/bin/env python3
import pandas as pd
import pika
import json
import os
import time

CSV_PATH       = os.environ.get("CSV_PATH", "./data/transactions_autoconnect.csv")
RABBITMQ_HOST  = os.environ.get("RABBITMQ_HOST", "rabbitmq")
NOMBRE_WORKERS = int(os.environ.get("NUM_WORKERS", "3"))
taille = 100  # Taille fixe de chaque batch

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

def scinder_donnees(df, taille=100):
    """Divise le DataFrame en batchs pour les workers."""
    print(f"[Parser] Division des données en batchs de {taille} lignes")
    batchs = [df.iloc[i:i+taille] for i in range(0, len(df), taille)]
    print(f"[Parser] Données divisées en {len(batchs)} batchs de {taille} lignes")
    return batchs

    # taille = len(df) // nb_workers + (1 if len(df) % nb_workers else 0)
    # batchs = [df.iloc[i:i+taille] for i in range(0, len(df), taille)]
    # print(f"[Parser] Données divisées en {len(batchs)} batchs")
    # return batchs

def distribuer_batchs(batchs):
    """Publie chaque batch sur la queue task."""
    total = len(batchs)
    for i, batch in enumerate(batchs):
        message = {
            "type":     "batch",
            "batch_id": i+1,
            "data":     batch.to_dict(orient="records")
        }
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[Parser] Batch {i+1}/{total} publié ({len(batch)} lignes)")
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
    batchs = scinder_donnees(df, taille=taille)
    total = distribuer_batchs(batchs)
    notifier_fin(total)
    channel.close()
    connection.close()

if __name__ == "__main__":
    main()

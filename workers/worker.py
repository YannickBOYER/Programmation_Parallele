#!/usr/bin/env python3
import pandas as pd
import pika
import json
import os
import time

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")

# Connexion à RabbitMQ
print("[Worker] Connexion à RabbitMQ…")
connection = None
for _ in range(10):
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST)
        )
        break
    except pika.exceptions.AMQPConnectionError:
        print("[Worker] Erreur de connexion, retry dans 10s…")
        time.sleep(10)
if not connection or not connection.is_open:
    raise RuntimeError("[Worker] Impossible de se connecter à RabbitMQ.")

channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='results',     durable=True)
channel.basic_qos(prefetch_count=1)

def calcul_ca_mensuel_par_ville(df):
    """Calcule le CA mensuel par ville."""
    df['date'] = pd.to_datetime(df['date'])
    df['prix'] = pd.to_numeric(df['prix'], errors='coerce')
    df['mois'] = df['date'].dt.strftime('%Y-%m')
    resultat = df.groupby(['ville','mois'])['prix'].sum().reset_index()
    ca = {}
    for _, row in resultat.iterrows():
        ville, mois, montant = row['ville'], row['mois'], float(row['prix'])
        ca.setdefault(ville, {})[mois] = montant
    return ca

def calcul_repartition_vente_location(df):
    """Calcule la répartition vente/location par ville."""
    resultat = df.groupby(['ville','type']).size().reset_index(name='count')
    repart = {}
    for _, row in resultat.iterrows():
        ville, ttype, cnt = row['ville'], row['type'], int(row['count'])
        repart.setdefault(ville, {})[ttype] = cnt
    return repart

def trouver_top_modeles(df, top_n=5):
    """Détermine les top modèles par ville."""
    resultat = df.groupby(['ville','modele']).size().reset_index(name='count')
    top_mod = {}
    for ville in resultat['ville'].unique():
        sub = resultat[resultat['ville']==ville]
        t5  = sub.sort_values('count', ascending=False).head(top_n)
        top_mod[ville] = {row['modele']: int(row['count']) for _, row in t5.iterrows()}
    return top_mod

def gestion_message(ch, method, props, body):
    """Callback pour traiter un batch."""
    msg = json.loads(body)
    batch_id = msg['batch_id']
    df       = pd.DataFrame.from_records(msg['data'])
    print(f"[Worker] Traitement batch {batch_id} ({len(df)} lignes)")

    resultats = {
        'ca_mensuel_ville':          calcul_ca_mensuel_par_ville(df),
        'repartition_vente_location': calcul_repartition_vente_location(df),
        'top_models':                trouver_top_modeles(df)
    }

    sortie = {
        "type":     "batch",
        "batch_id": batch_id,
        "results":  resultats
    }
    ch.basic_publish(
        exchange='',
        routing_key='results',
        body=json.dumps(sortie),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print(f"[Worker] Résultats batch {batch_id} publiés.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    channel.basic_consume(queue='task_queue', on_message_callback=gestion_message)
    print("[Worker] En attente de tâches…")
    channel.start_consuming()

if __name__ == "__main__":
    main()

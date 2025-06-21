#!/usr/bin/env python3
import pika
import json
import os
from collections import defaultdict
import time

RABBITMQ_HOST     = os.environ.get("RABBITMQ_HOST", "rabbitmq")
QUEUE_RESULTS     = 'results'
QUEUE_AGGREGATED  = 'aggregated_results'

# État global (un seul job à la fois)
liste_resultats = []
ids_recus       = set()
attendu         = None

def fusion_ca_mensuels(liste):
    fusion = defaultdict(lambda: defaultdict(float))
    for r in liste:
        for ville, md in r.get('ca_mensuel_ville', {}).items():
            for mois, c in md.items():
                fusion[ville][mois] += c
    return {v: dict(m) for v, m in fusion.items()}

def fusion_repartition_vente_location(liste):
    fusion = defaultdict(lambda: defaultdict(int))
    for r in liste:
        for ville, td in r.get('repartition_vente_location', {}).items():
            for t, c in td.items():
                fusion[ville][t] += c
    return {v: dict(t) for v, t in fusion.items()}

def fusion_top_modeles(liste, top_n=5):
    compt = defaultdict(lambda: defaultdict(int))
    for r in liste:
        for ville, md in r.get('top_models', {}).items():
            for m, c in md.items():
                compt[ville][m] += c
    out = {}
    for ville, md in compt.items():
        top5 = sorted(md.items(), key=lambda x: x[1], reverse=True)[:top_n]
        out[ville] = dict(top5)
    return out

def calcul_pourcentages_vente_location(repart):
    pct = {}
    for ville, td in repart.items():
        total = sum(td.values())
        pct[ville] = {t: round(c/total*100,2) for t,c in td.items()} if total else {}
    return pct

def gestion_message(channel, method, props, body):
    global attendu, liste_resultats, ids_recus

    msg = json.loads(body)
    t   = msg.get('type')

    if t == 'batch':
        id_batch = msg['batch_id']
        liste_resultats.append(msg['results'])
        ids_recus.add(id_batch)
        print(f"[Aggregator] Reçu batch {id_batch}")

    elif t == 'FIN':
        attendu = msg['total_batches']
        print(f"[Aggregator] Signal FIN reçu (total={attendu})")

    channel.basic_ack(delivery_tag=method.delivery_tag)

    # Dès qu'on a tous les batchs
    if attendu is not None and len(ids_recus) == attendu:
        print("[Aggregator] Tous les batchs reçus, agrégation…")
        agg = {
            'ca_mensuel_ville':          fusion_ca_mensuels(liste_resultats),
            'repartition_vente_location': fusion_repartition_vente_location(liste_resultats),
            'top_models':                fusion_top_modeles(liste_resultats)
        }
        agg['pourcentage_vente_location'] = calcul_pourcentages_vente_location(
            agg['repartition_vente_location']
        )

        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_AGGREGATED,
            body=json.dumps(agg),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print("[Aggregator] Résultat agrégé publié.")
        # réinitialisation
        liste_resultats.clear()
        ids_recus.clear()
        attendu = None

def main():
    # Connexion à RabbitMQ
    # Attend que le serveur RabbitMQ soit prêt
    print("[Aggregator] Connexion à RabbitMQ...")
    for _ in range(10):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            break
        except pika.exceptions.AMQPConnectionError:
            print("[Aggregator] Erreur de connexion, en attente de RabbitMQ...")
            time.sleep(10)

    if not connection:
        raise Exception("[Aggregator] Impossible de se connecter à RabbitMQ.")
        
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_RESULTS,    durable=True)
    channel.queue_declare(queue=QUEUE_AGGREGATED, durable=True)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue=QUEUE_RESULTS, on_message_callback=gestion_message)
    print("[Aggregator] En attente de 'results'…")
    channel.start_consuming()

if __name__ == "__main__":
    main()

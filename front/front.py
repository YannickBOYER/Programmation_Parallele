#!/usr/bin/env python3
import os
import json
import pika
from flask import Flask, jsonify, abort

app = Flask(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = 'aggregated_results'

def recuperer_resultats_agreges():
    """
    Récupère les résultats agrégés depuis RabbitMQ.
    Si la file d'attente est vide, retourne un dictionnaire vide.
    Si un message est trouvé, le consomme et le renvoie.
    """
    connexion = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    canal = connexion.channel()
    canal.queue_declare(queue=QUEUE_NAME, durable=True)

    method_frame, header_frame, body = canal.basic_get(queue=QUEUE_NAME, auto_ack=False)
    if method_frame:
        data = json.loads(body)
        canal.basic_nack(method_frame.delivery_tag, requeue=True)
    else:
        data = {}
    connexion.close()
    return data

def require_data(part):
    data = recuperer_resultats_agreges()
    if not data:
        abort(404, description="Pas de données agrégées disponibles.")
    return data.get(part, {})

@app.route('/ca_mensuel_ville')
def route_ca_mensuel():
    """
    Retourne {"ville": { "YYYY-MM": ca, ...}, ...}
    """
    return jsonify(require_data('ca_mensuel_ville'))

@app.route('/repartition_vente_location')
def route_repartition():
    """
    Retourne {"ville": { "vente": count, "location": count, ...}, ...}
    """
    return jsonify(require_data('repartition_vente_location'))

@app.route('/top_models')
def route_top_models():
    """
    Retourne {"ville": { "modele1": count, ...}, ...}
    """
    return jsonify(require_data('top_models'))

@app.route('/pourcentage_vente_location')
def route_pourcentages():
    """
    Retourne {"ville": { "vente": pct, "location": pct, ...}, ...}
    """
    return jsonify(require_data('pourcentage_vente_location'))

if __name__ == "__main__":
    # Exécute : flask run --host=0.0.0.0 --port=5000
    app.run(host="0.0.0.0", port=5000)

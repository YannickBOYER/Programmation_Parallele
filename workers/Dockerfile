# Utilise une image Python officielle
FROM python:3.11-slim

# Répertoire de travail
WORKDIR /app

# Copie le script Python dans le conteneur
COPY . .

# Installe les dépendances nécessaires
RUN pip install pika

# Définit la commande de lancement
CMD ["python", "worker.py"]

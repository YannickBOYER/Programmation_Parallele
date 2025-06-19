# Utilise une image Python officielle
FROM python:3.11-slim

# Crée un dossier de travail
WORKDIR /app

# Copie les fichiers du projet dans l'image Docker
COPY . .

# Installe les dépendances (si besoin tu peux en ajouter)
RUN pip install pika

# Définit le point d’entrée
CMD ["python", "aggregator.py"]

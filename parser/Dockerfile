FROM python:3.11-slim

# Définit le dossier de travail
WORKDIR /app

# Copie les fichiers du projet dans l'image Docker
COPY . .

# Installe les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Définit le point d’entrée
CMD ["python", "parser.py"]

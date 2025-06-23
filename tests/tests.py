# Avec le csv ./data/transactions_autoconnect.csv, peux tu me calculer la repartition de la colonne type par ville ?
import pandas as pd
if __name__ == "__main__":
    df = pd.read_csv('./data/transactions_autoconnect.csv', dtype=str)
    repartition = df.groupby(['ville', 'type']).size().reset_index(name='count')
    print("Répartition par ville et type :")
    print(repartition)

    print("\n")

    # Maintenant, pour chaque ville, fais la somme des chiffres d'affaires mensuels sachant que prix est un string
    df['prix'] = pd.to_numeric(df['prix'], errors='coerce')  # Convertit les prix en numérique, gère les erreurs
    df['prix'] = df['prix'].fillna(0)  # Remplace
    df['date'] = pd.to_datetime(df['date'], errors='coerce')  # Convertit les dates, gère les erreurs
    df['mois'] = df['date'].dt.strftime('%Y-%m')  # Extrait le mois au format YYYY-MM
    ca_mensuel = df.groupby(['ville', 'mois'])['prix'].sum().reset_index()
    print("Chiffre d'affaires mensuel par ville :")
    print(ca_mensuel)
    

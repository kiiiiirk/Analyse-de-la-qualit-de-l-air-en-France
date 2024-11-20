import pandas as pd
import matplotlib.pyplot as plt
import os

# Définir une palette de couleurs harmonisée pour les polluants
colors = {
    'NO2': 'blue',
    'O3': 'green',
    'PM10': 'orange',
    'PM2.5': 'red'
}

# Chemin absolu du fichier CSV
file_path = "données/villes_pollution_population(FINAL).csv"

# Vérifier que le fichier existe
if not os.path.exists(file_path):
    raise FileNotFoundError(f"Le fichier n'existe pas : {file_path}")

# Charger le fichier
df = pd.read_csv(file_path)

# Vérifier que les colonnes nécessaires existent
if all(col in df.columns for col in ['NO2', 'O3', 'PM10', 'PM2.5', 'p24_pop', 'City']):
    # Filtrer les données avec des valeurs valides pour tous les polluants
    df_filtered = df.dropna(subset=['NO2', 'O3', 'PM10', 'PM2.5', 'p24_pop'])

    # Tracer le nuage de points
    plt.figure(figsize=(14, 10))

    # Tracer les points pour chaque polluant avec les mêmes couleurs
    for polluant, color in colors.items():
        plt.scatter(df_filtered['p24_pop'], df_filtered[polluant], color=color, alpha=0.6, label=polluant)

    # Ajouter les noms des villes pour les plus grandes ou les plus polluées
    for i, row in df_filtered.iterrows():
        if (row['p24_pop'] > 150000 or
                row['NO2'] > 60 or
                row['O3'] > 80 or
                row['PM10'] > 50 or
                row['PM2.5'] > 25):
            plt.text(row['p24_pop'], row['NO2'], str(row['City']), fontsize=8, alpha=0.7)

    # Ajuster les échelles
    plt.xscale('log')  # Échelle logarithmique pour la population

    # Ajouter des labels, une légende et un titre
    plt.xlabel("Population (2024)")
    plt.ylabel("Concentration des polluants")
    plt.title("Pollution des principales villes en fonction de leur population (2024)")
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))  # Légende en dehors du graphique
    plt.grid(True, linestyle='--', alpha=0.5)

    # Afficher le graphique
    plt.tight_layout()
    plt.show()
else:
    print("Certaines colonnes nécessaires ('NO2', 'O3', 'PM10', 'PM2.5', 'p24_pop', 'City') ne sont pas présentes dans le fichier.")

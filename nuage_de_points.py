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

# Définir les seuils pour chaque polluant
seuils = {
    'NO2': 60,
    'O3': 80,
    'PM10': 50,
    'PM2.5': 25
}

# Chemin du fichier CSV
file_path = "données/villes_pollution_population(FINAL).csv"

# Vérifier que le fichier existe
if not os.path.exists(file_path):
    raise FileNotFoundError(f"Le fichier n'existe pas : {file_path}")

# Charger le fichier
df = pd.read_csv(file_path)

# Vérifier que les colonnes nécessaires existent
colonnes_necessaires = ['NO2', 'O3', 'PM10', 'PM2.5', 'p24_pop', 'City']
if all(col in df.columns for col in colonnes_necessaires):
    # Filtrer les données avec des valeurs valides pour tous les polluants
    df_filtered = df.dropna(subset=colonnes_necessaires)

    # Tracer le nuage de points
    plt.figure(figsize=(14, 10))

    # Tracer les points pour chaque polluant avec les mêmes couleurs
    for polluant, color in colors.items():
        plt.scatter(df_filtered['p24_pop'], df_filtered[polluant], color=color, alpha=0.6, label=polluant)

        # Ajouter les noms des villes pour les plus grandes ou les plus polluées pour ce polluant
        for i, row in df_filtered.iterrows():
            if row['p24_pop'] > 150000 or row[polluant] > seuils[polluant]:
                plt.text(row['p24_pop'], row[polluant], str(row['City']), fontsize=8, alpha=0.7)

    # Ajuster les échelles
    plt.xscale('log')  # Échelle logarithmique pour la population

    # Ajouter des labels, une légende et un titre
    plt.xlabel("Population (2024)")
    plt.ylabel("Concentration des polluants (µg/m³)")
    plt.title("Pollution des principales villes en fonction de leur population (2024)")
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))  # Légende en dehors du graphique
    plt.grid(True, linestyle='--', alpha=0.5)

    # Ajuster les marges pour éviter que la légende ne coupe le graphique
    plt.tight_layout()

    # Afficher le graphique
    plt.show()
    plt.savefig('nuage_de_points.png', dpi=300)
else:
    print(f"Certaines colonnes nécessaires {colonnes_necessaires} ne sont pas présentes dans le fichier.")

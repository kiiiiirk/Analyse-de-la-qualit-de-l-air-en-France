import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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
if all(col in df.columns for col in ['NO2', 'O3', 'PM10', 'PM2.5', 'Altitude Moyenne']):
    # Créer des catégories d'altitude
    bins = [0, 200, 500, 1000, 2000, 3000]  # Tranches d'altitude (en mètres)
    labels = ['0-200m', '200-500m', '500-1000m', '1000-2000m', '2000-3000m']
    df['Altitude_Catégorie'] = pd.cut(df['Altitude Moyenne'], bins=bins, labels=labels)

    # Filtrer les données valides
    df_filtered = df.dropna(subset=['NO2', 'O3', 'PM10', 'PM2.5', 'Altitude_Catégorie'])

    # Réorganiser les données pour un graphique groupé
    df_melted = df_filtered.melt(
        id_vars='Altitude_Catégorie',
        value_vars=['NO2', 'O3', 'PM10', 'PM2.5'],
        var_name='Polluant',
        value_name='Concentration'
    )

    # Configurer le graphique
    plt.figure(figsize=(14, 8))
    sns.boxplot(
        x='Altitude_Catégorie',
        y='Concentration',
        hue='Polluant',
        data=df_melted,
        palette=colors,  # Utiliser les mêmes couleurs
        showfliers=False
    )

    # Ajuster les labels et le titre
    plt.xlabel("Catégories d'altitude")
    plt.ylabel("Concentration des polluants (µg/m³)")
    plt.title("Concentration des polluants par catégories d'altitude")
    plt.legend(title="Polluant")
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()

    # Afficher le graphique
    plt.show()
else:
    print("Certaines colonnes nécessaires ('NO2', 'O3', 'PM10', 'PM2.5', 'Altitude Moyenne') ne sont pas présentes dans le fichier.")

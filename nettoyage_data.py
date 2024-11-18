import pandas as pd

# Définir le chemin vers le dossier "données"
data_path = "./données/"  # Ajustez ce chemin si nécessaire
 

# Charger les fichiers bruts
population_2024 = pd.read_csv(f"{data_path}population2024.csv")  # Fichier population2024.csv
population_2016 = pd.read_csv(f"{data_path}population2016.csv", delimiter=';')  # Fichier population2016.csv

# Convertir `Code INSEE` en type numérique pour éviter les incohérences
population_2024['code_commune_INSEE'] = pd.to_numeric(population_2024['code_commune_INSEE'], errors='coerce')
population_2016['Code INSEE'] = pd.to_numeric(population_2016['Code INSEE'], errors='coerce')

# Filtrer les colonnes nécessaires pour chaque fichier
population_2024 = population_2024[
    ['code_commune_INSEE', 'code_postal', 'code_commune', 'surface', 'population']
]
population_2016 = population_2016[
    ['Nom de la région', 'Code département', 'Nom de la commune', 'Population totale', 'Code INSEE']
]

# Renommer les colonnes pour correspondre à une clé commune et à une meilleure lecture
population_2024 = population_2024.rename(columns={'code_commune_INSEE': 'Code INSEE', 'population': 'population_2024', 'surface': 'superficie'})
population_2016 = population_2016.rename(columns={'Population totale': 'population_2016'})

# Nettoyage de population_2024
colonnes_essentielles_2024 = ['Code INSEE', 'code_postal', 'code_commune', 'superficie', 'population_2024']
population_2024_cleaned = (
    population_2024
    .drop_duplicates(subset=['Code INSEE'])  # Suppression des doublons basés sur `Code INSEE`
    .dropna(subset=colonnes_essentielles_2024)  # Suppression des lignes avec des valeurs manquantes dans les colonnes essentielles
)
population_2024_cleaned.to_csv(f"{data_path}population2024_cleaned.csv", index=False)
print(f"Fichier population2024_cleaned.csv enregistré avec {len(population_2024_cleaned)} lignes.")

# Nettoyage de population_2016
colonnes_essentielles_2016 = ['Code INSEE', 'Nom de la région', 'Code département', 'Nom de la commune', 'population_2016']
population_2016_cleaned = (
    population_2016
    .drop_duplicates(subset=['Code INSEE'])  # Suppression des doublons basés sur `Code INSEE`
    .dropna(subset=colonnes_essentielles_2016)  # Suppression des lignes avec des valeurs manquantes dans les colonnes essentielles
)
population_2016_cleaned.to_csv(f"{data_path}population2016_cleaned.csv", index=False)
print(f"Fichier population2016_cleaned.csv enregistré avec {len(population_2016_cleaned)} lignes.")

# Fusion des fichiers nettoyés
fusion_finale = pd.merge(population_2024_cleaned, population_2016_cleaned, on='Code INSEE', how='inner')

# Réorganiser les colonnes pour mettre les colonnes de population côte à côte et à la fin
cols = [col for col in fusion_finale.columns if col not in ['population_2016', 'population_2024']] + ['population_2016', 'population_2024']
fusion_finale = fusion_finale[cols]

# Exporter la fusion finale
fusion_finale.to_csv(f"{data_path}population_fr_2016_2024.csv", index=False)
print(f"Fichier de fusion finale (population_fr_2016_2024.csv) enregistré avec {len(fusion_finale)} lignes.")



# Charger les données avec toutes les colonnes
villes_polluants = pd.read_csv(f"{data_path}villes_polluants.csv")

# Convertir la colonne 'LastUpdated' en datetime
villes_polluants['LastUpdated'] = pd.to_datetime(villes_polluants['LastUpdated'])

# Vérifier si 'Postal_Code' contient des valeurs manquantes
if villes_polluants['Postal_Code'].isnull().any():
    raise ValueError("Certaines lignes ne contiennent pas de code postal. Veuillez nettoyer les données avant de continuer.")

# Afficher les premières lignes pour vérifier l'importation
print("Aperçu des données :")
print(villes_polluants.head())

# Pivoter la table
table_pivot = villes_polluants.pivot_table(
    index=['Postal_Code', 'City', 'LastUpdated'],  # Utilisation de Postal_Code, City et LastUpdated
    columns='Pollutant',
    values='value',
    aggfunc='mean'
)

# Réinitialiser l'index pour revenir à une structure tabulaire
table_pivot.reset_index(inplace=True)

# Réintégrer les colonnes non pivotées
non_pivot_columns = ['Country.Code', 'Location', 'Latitude', 'Longitude', 'Country.Label', 'Department', 'Region']
for col in non_pivot_columns:
    if col in villes_polluants.columns and col not in table_pivot.columns:
        table_pivot[col] = villes_polluants[col].iloc[:len(table_pivot)].values

# Supprimer les colonnes NO et CO
table_pivot.drop(columns=['NO', 'CO'], inplace=True, errors='ignore')
print("Colonnes 'NO' et 'CO' supprimées.")

# Trier par 'Postal_Code', 'City' et 'LastUpdated'
table_pivot.sort_values(by=['Postal_Code', 'City', 'LastUpdated'], inplace=True)

# Mise en index pour l'interpolation
table_pivot.set_index('LastUpdated', inplace=True)

# Appliquer l'interpolation par groupe
def interpolate_group(group):
    # Filtrer les colonnes numériques
    numeric_cols = group.select_dtypes(include=['float', 'int']).columns
    # Appliquer l'interpolation temporelle
    group[numeric_cols] = group[numeric_cols].interpolate(method='time', limit_direction='both')
    return group

# Appliquer l'interpolation sur les groupes combinés (Postal_Code et City)
table_pivot = table_pivot.groupby(['Postal_Code', 'City'], group_keys=False).apply(interpolate_group)

# Réinitialiser l'index après interpolation
table_pivot.reset_index(inplace=True)

# Vérifier s'il reste des valeurs manquantes
valeurs_restantes = table_pivot.isnull().sum().sum()
if valeurs_restantes == 0:
    print("Toutes les valeurs manquantes ont été corrigées par interpolation temporelle (Postal_Code + City).")
else:
    print(f"Il reste encore {valeurs_restantes} valeurs manquantes après interpolation.")

# Aperçu de la table après traitement
print("\nAperçu des données après interpolation :")
print(table_pivot.head())

# Exporter les résultats
output_file = f"{data_path}villes_polluants_cleaned.csv"
table_pivot.to_csv(output_file, index=False)
print(f"Fichier nettoyé exporté sous : {output_file}")



'''
import pandas as pd

# Charger les fichiers
file_polluants = f"{data_path}villes_polluants_interpolated_by_postal_code_and_city.csv"
file_population = f"{data_path}population_fr_2016_2024.csv"

# Charger les tables
polluants_table = pd.read_csv(file_polluants)
population_table = pd.read_csv(file_population)

# Vérifier les colonnes
print("Colonnes de la table polluants :")
print(polluants_table.columns)

print("\nColonnes de la table population :")
print(population_table.columns)

# Harmoniser les colonnes pour la fusion
# Renommer 'code_postal' en 'Postal_Code' dans la table population
population_table.rename(columns={'code_postal': 'Postal_Code'}, inplace=True)

# Convertir les colonnes 'Postal_Code' en chaîne pour éviter les erreurs de type
polluants_table['Postal_Code'] = polluants_table['Postal_Code'].astype(str)
population_table['Postal_Code'] = population_table['Postal_Code'].astype(str)

# Effectuer la fusion
merged_table = pd.merge(
    polluants_table,
    population_table,
    on='Postal_Code',
    how='inner'  # Garder uniquement les correspondances entre les deux tables
)

# Vérifier les données fusionnées
print("\nAperçu de la table fusionnée :")
print(merged_table.head())

# Exporter la table fusionnée
output_file = f"{data_path}villes_polluants_population_merged.csv"
merged_table.to_csv(output_file, index=False)
print(f"Table fusionnée exportée sous : {output_file}")

'''

import pandas as pd

# Définir le chemin vers le dossier "données"
data_path = "./données/"  # Ajustez ce chemin si nécessaire

# Charger les fichiers CSV
df_correspondance = pd.read_csv(f"{data_path}correspondance-code-insee-code-postal.csv", encoding='ISO-8859-1', delimiter=';')
df_population = pd.read_csv(f"{data_path}POPULATION_MUNICIPALE_COMMUNES_FRANCE.csv", encoding='ISO-8859-1')

# Afficher les colonnes pour vérifier les noms
print("Colonnes dans df_correspondance :")
print(df_correspondance.columns)

print("\nColonnes dans df_population :")
print(df_population.columns)

# Nettoyer les noms des colonnes pour enlever les espaces et caractères invisibles
df_correspondance.columns = df_correspondance.columns.str.strip()  # Supprimer les espaces et caractères invisibles
df_population.columns = df_population.columns.str.strip()

# Corriger le problème de BOM dans le nom de la colonne 'ï»¿Code INSEE' dans df_correspondance
df_correspondance.columns = df_correspondance.columns.str.replace('ï»¿', '', regex=False)

# Vérifier les colonnes après nettoyage
print("\nColonnes après nettoyage dans df_correspondance :")
print(df_correspondance.columns)

print("\nColonnes après nettoyage dans df_population :")
print(df_population.columns)

# Renommer la colonne 'codgeo' dans df_population pour qu'elle corresponde à 'Code INSEE'
df_population.rename(columns={'codgeo': 'Code INSEE'}, inplace=True)

# Vérification des doublons dans df_correspondance avant la fusion
duplicates_correspondance = df_correspondance.duplicated(subset='Code INSEE', keep=False)  # Identifie les doublons
duplicates_population = df_population.duplicated(subset='Code INSEE', keep=False)  # Identifie les doublons

# Afficher le nombre de doublons supprimés dans chaque DataFrame
print(f"Doublons dans df_correspondance : {duplicates_correspondance.sum()}")
print(f"Doublons dans df_population : {duplicates_population.sum()}")

# Supprimer les doublons dans chaque DataFrame avant la fusion
df_correspondance.drop_duplicates(subset='Code INSEE', keep='first', inplace=True)
df_population.drop_duplicates(subset='Code INSEE', keep='first', inplace=True)

# Supprimer les lignes avec des NaN dans les colonnes 'Code INSEE', 'Code Postal' et les colonnes de population (qui commencent par 'pop')
df_correspondance.dropna(subset=['Code INSEE', 'Code Postal'], inplace=True)
df_population.dropna(subset=['Code INSEE'] + [col for col in df_population.columns if col.startswith('p')], inplace=True)

# Vérification des valeurs manquantes après nettoyage
print("\nValeurs manquantes dans le fichier correspondance après nettoyage :")
print(df_correspondance.isnull().sum())

print("\nValeurs manquantes dans le fichier population après nettoyage :")
print(df_population.isnull().sum())

# Fusionner les deux DataFrames par 'Code INSEE'
merged_df = pd.merge(df_correspondance, df_population, on='Code INSEE', how='inner')

# Supprimer les colonnes non nécessaires après la fusion
columns_to_drop = ['DÃ©partement', 'RÃ©gion', 'Statut', 'Population', 'geo_point_2d', 'geo_shape', 'ID Geofla', 
                   'Code Commune', 'Code Canton', 'Code Arrondissement', 'Code DÃ©partement', 'Code RÃ©gion', 'objectid', 
                   'reg', 'dep', 'cv', 'libgeo', 'p13_pop', 'p14_pop', 'p15_pop']
merged_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

# Afficher un aperçu des premières lignes après la fusion et suppression des colonnes
print("\nAperçu de la table fusionnée après suppression des colonnes inutiles :")
print(merged_df.head())

# Exporter le fichier fusionné en CSV
output_file = f"{data_path}population_2016_2024.csv"
merged_df.to_csv(output_file, index=False)  # index=False pour ne pas inclure l'index dans le fichier CSV
print(f"Fichier fusionné exporté sous : {output_file}")

'''
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

##A MODIFIER(CEST PAS LA BONNE BASE)
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

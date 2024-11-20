import pandas as pd

# --------------------
# Étape 1 : Traitement des données de population
# --------------------

# Définir le chemin vers le dossier "données"
data_path = "./données/"  # Ajustez ce chemin si nécessaire

# Charger les fichiers CSV
df_correspondance = pd.read_csv(
    f"{data_path}correspondance-code-insee-code-postal.csv",
    encoding='ISO-8859-1',
    delimiter=';'
)
df_population_p2 = pd.read_csv(
    f"{data_path}population_p2.csv",
    encoding='ISO-8859-1'
)

# Nettoyer les noms des colonnes pour enlever les espaces et caractères invisibles
df_correspondance.columns = df_correspondance.columns.str.strip()
df_population_p2.columns = df_population_p2.columns.str.strip()

# Corriger le problème de BOM dans le nom de la colonne 'ï»¿Code INSEE' dans df_correspondance
df_correspondance.columns = df_correspondance.columns.str.replace('ï»¿', '', regex=False)

# Renommer les colonnes dans df_population_p2 pour correspondre à df_correspondance
df_population_p2.rename(
    columns={
        'code_commune_INSEE': 'Code INSEE',
        'code_postal': 'Code Postal',
        'nom_commune': 'Commune',
        'surface': 'Superficie',
        'population': 'Population'
    },
    inplace=True
)

# Vérifier si 'Code INSEE' est bien dans df_population_p2
if 'Code INSEE' not in df_population_p2.columns:
    raise KeyError("La colonne 'Code INSEE' n'existe pas dans df_population_p2. Vérifiez le nom de la colonne.")

# *** Conversion des types de données pour les colonnes de fusion ***
# Convertir 'Code INSEE' et 'Code Postal' en chaînes de caractères dans les deux DataFrames
df_correspondance['Code INSEE'] = df_correspondance['Code INSEE'].astype(str)
df_correspondance['Code Postal'] = df_correspondance['Code Postal'].astype(str)
df_population_p2['Code INSEE'] = df_population_p2['Code INSEE'].astype(str)
df_population_p2['Code Postal'] = df_population_p2['Code Postal'].astype(str)

# Supprimer les doublons dans chaque DataFrame avant la fusion
df_correspondance.drop_duplicates(subset=['Code INSEE', 'Code Postal'], keep='first', inplace=True)
df_population_p2.drop_duplicates(subset=['Code INSEE', 'Code Postal'], keep='first', inplace=True)

# Supprimer les lignes avec des NaN dans les colonnes clés
df_correspondance.dropna(subset=['Code INSEE', 'Code Postal', 'Altitude Moyenne'], inplace=True)
df_population_p2.dropna(subset=['Code INSEE', 'Code Postal', 'Population', 'Superficie'], inplace=True)

# Fusionner les deux DataFrames par 'Code INSEE' et 'Code Postal'
merged_df = pd.merge(
    df_correspondance,
    df_population_p2,
    on=['Code INSEE', 'Code Postal'],
    how='inner',
    suffixes=('_correspondance', '_population')
)

# Conserver la colonne 'Commune' après la fusion
if 'Commune_population' in merged_df.columns:
    merged_df['Commune'] = merged_df['Commune_population']
elif 'Commune_correspondance' in merged_df.columns:
    merged_df['Commune'] = merged_df['Commune_correspondance']

# *** Ne pas supprimer la colonne 'Altitude' ***
# Supprimer les colonnes non nécessaires après la fusion
columns_to_drop = [
    'Statut', 'geo_point_2d', 'geo_shape', 'ID Geofla',
    'Code Commune', 'Code Canton', 'Code Arrondissement',
    'Population_correspondance', 'Population_population',
    'Superficie_correspondance', 'Superficie_population',
    'Commune_correspondance', 'Commune_population'
]
merged_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

# Supprimer les colonnes spécifiées par l'utilisateur
columns_to_remove = ['Département', 'Région', 'Code Département', 'Code Région']
merged_df.drop(columns=columns_to_remove, inplace=True, errors='ignore')

# Exporter le fichier fusionné en CSV
output_file = f"{data_path}population_p1.csv"
merged_df.to_csv(output_file, index=False)
print(f"Fichier fusionné exporté sous : {output_file}")

# --------------------
# Étape 2 : Traitement des données de polluants
# --------------------

# Charger les fichiers CSV
villes_polluants = pd.read_csv(f"{data_path}villes_polluants.csv")

# Convertir la colonne 'LastUpdated' en datetime
villes_polluants['LastUpdated'] = pd.to_datetime(villes_polluants['LastUpdated'])

# Vérifier si 'Postal_Code' contient des valeurs manquantes
if villes_polluants['Postal_Code'].isnull().any():
    raise ValueError("Certaines lignes ne contiennent pas de code postal. Veuillez nettoyer les données avant de continuer.")

# Pivoter la table
table_pivot = villes_polluants.pivot_table(
    index=['Postal_Code', 'City', 'LastUpdated'],
    columns='Pollutant',
    values='value',
    aggfunc='mean'
)

# Réinitialiser l'index pour revenir à une structure tabulaire
table_pivot.reset_index(inplace=True)

# Fusionner les informations non pivotées avant le pivotement
non_pivot_columns = ['Country.Code', 'Location', 'Latitude', 'Longitude', 'Country.Label', 'Department', 'Region']

# Créer une table contenant uniquement les colonnes non pivotées avec les mêmes index
non_pivot_data = villes_polluants[non_pivot_columns + ['Postal_Code', 'City', 'LastUpdated']].drop_duplicates(
    subset=['Postal_Code', 'City', 'LastUpdated']
)

# Fusionner les données non pivotées avec la table pivotée
table_pivot = pd.merge(table_pivot, non_pivot_data, on=['Postal_Code', 'City', 'LastUpdated'], how='left')

# Trier par 'Postal_Code', 'City' et 'LastUpdated'
table_pivot.sort_values(by=['Postal_Code', 'City', 'LastUpdated'], inplace=True)

# Mise en index pour l'interpolation
table_pivot.set_index('LastUpdated', inplace=True)

# Appliquer l'interpolation par groupe avec Postal_Code, City, Department
def interpolate_group(group):
    numeric_cols = group.select_dtypes(include=['float', 'int']).columns
    group[numeric_cols] = group[numeric_cols].interpolate(method='time', limit_direction='both')
    return group

table_pivot = table_pivot.groupby(['Postal_Code', 'City', 'Department'], group_keys=False).apply(interpolate_group)

# Réinitialiser l'index après interpolation
table_pivot.reset_index(inplace=True)

# Supprimer les colonnes 'CO' et 'NO' si elles existent
table_pivot.drop(columns=['CO', 'NO'], inplace=True, errors='ignore')
print("Colonnes 'CO' et 'NO' supprimées.")

# Exporter les résultats
output_file = f"{data_path}villes_polluants_cleaned.csv"
table_pivot.to_csv(output_file, index=False)
print(f"Fichier nettoyé exporté sous : {output_file}")

# --------------------
# Étape 3 : Fusion des données de pollution et de population
# --------------------

# Charger les fichiers CSV
df_villes_polluants = pd.read_csv(f"{data_path}villes_polluants_cleaned.csv")
df_population = pd.read_csv(f"{data_path}population_p1.csv", encoding='ISO-8859-1')

# Nettoyer les noms des colonnes pour enlever les espaces et caractères invisibles
df_villes_polluants.columns = df_villes_polluants.columns.str.strip()
df_population.columns = df_population.columns.str.strip()

# Vérifier que 'Commune' est bien présent dans df_population
if 'Commune' not in df_population.columns:
    raise KeyError("La colonne 'Commune' est manquante dans df_population. Vérifiez la fusion précédente.")

# Préparer les noms des villes pour la fusion
df_villes_polluants['City_short'] = df_villes_polluants['City'].str[:5].str.lower()
df_population['Commune_short'] = df_population['Commune'].str[:5].str.lower()

# Fonction pour vérifier et corriger les codes postaux si plusieurs sont présents dans la table population
def fix_postal_code(row, villes_polluants):
    if '/' in str(row['Code Postal']):
        postal_codes = row['Code Postal'].split('/')
        for postal_code in postal_codes:
            postal_code = postal_code.strip()
            if postal_code in villes_polluants['Postal_Code'].astype(str).values:
                return postal_code
    return row['Code Postal']

df_population['Code Postal'] = df_population.apply(fix_postal_code, villes_polluants=df_villes_polluants, axis=1)

# Convertir les codes postaux en chaîne de caractères pour éviter les problèmes de fusion
df_villes_polluants['Postal_Code'] = df_villes_polluants['Postal_Code'].astype(str)
df_population['Code Postal'] = df_population['Code Postal'].astype(str)

# Fusionner les deux tables en utilisant Postal_Code et les 5 premiers caractères des noms de villes
merged_df = pd.merge(
    df_villes_polluants,
    df_population,
    left_on=['Postal_Code', 'City_short'],
    right_on=['Code Postal', 'Commune_short'],
    how='inner'
)

# Supprimer les colonnes inutiles après la fusion
columns_to_drop = [
    'City_short', 'Commune', 'Commune_short', 'Code Postal', 'Code INSEE',
    'Country.Code', 'Location'
]
merged_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

# Exporter le fichier fusionné
output_file = f"{data_path}villes_population_1.csv"
merged_df.to_csv(output_file, index=False)
print(f"Fichier fusionné exporté sous : {output_file}")

# --------------------
# Étape 4 : Traitement final avec les données de population de 2024
# --------------------

# Charger les fichiers CSV
df_merged_villes_polluants_population = pd.read_csv(f"{data_path}villes_population_1.csv")
df_population_2024 = pd.read_csv(f"{data_path}population_p2.csv", encoding='ISO-8859-1')

# Nettoyer les noms des colonnes pour enlever les espaces et caractères invisibles
df_merged_villes_polluants_population.columns = df_merged_villes_polluants_population.columns.str.strip()
df_population_2024.columns = df_population_2024.columns.str.strip()

# Renommer les colonnes dans df_population_2024 pour correspondre
df_population_2024.rename(
    columns={
        'code_postal': 'Postal_Code',
        'population': 'p24_pop',
        'surface': 'Superficie',
        'nom_commune': 'Commune'
    },
    inplace=True
)

# Supprimer les arrondissements des villes cibles (Paris, Marseille, Lyon) sauf la ville elle-même
df_population_2024 = df_population_2024[
    ~df_population_2024['Commune'].str.contains(
        r'^(?:Paris|Marseille|Lyon) [0-9]+(?:er|ème|e)?$',
        na=False
    )
]

# Supprimer les lignes où 'p24_pop' ou 'Postal_Code' est manquant
df_population_2024.dropna(subset=['p24_pop', 'Postal_Code'], inplace=True)

# Convertir 'Postal_Code' en chaîne de caractères
df_population_2024['Postal_Code'] = df_population_2024['Postal_Code'].astype(str)
df_merged_villes_polluants_population['Postal_Code'] = df_merged_villes_polluants_population['Postal_Code'].astype(str)

# Filtrer les lignes de df_population_2024 qui ont un code postal correspondant
df_population_2024_filtered = df_population_2024[
    df_population_2024['Postal_Code'].isin(df_merged_villes_polluants_population['Postal_Code'])
]

# Réduire les doublons pour n'avoir qu'un enregistrement par code postal
df_population_2024_filtered = df_population_2024_filtered.drop_duplicates(subset='Postal_Code')

# Fusionner avec df_merged_villes_polluants_population sur 'Postal_Code'
final_merged_df = pd.merge(
    df_merged_villes_polluants_population,
    df_population_2024_filtered[['Postal_Code', 'p24_pop', 'Superficie']],
    on='Postal_Code',
    how='left'
)

# Modification des valeurs de 'p24_pop' pour certains codes postaux spécifiques
valeurs_a_inserer = {
    '13200': 52729,
    '20000': 71361,
    '31100': 498003,
    '33000': 268138,
    '34000': 315336,
    '35000': 221272,
    '37100': 137607,
    '38000': 158198,
    '42000': 171924,
    '44000': 333987,
    '50100': 79144,
    '54000': 104885,
    '57000': 122696,
    '59000': 238381,
    '59240': 85751,
    '63000': 144751,
    '66000': 122791,
    '67000': 290576,
    '68100': 108312,
    '73440': 2800,
    '74190': 11500,
    '93200': 115315,
    '87000': 127823,
    '84000': 89519,
    '80000': 134026,
    '76600': 163087,
    '76200': 27599,
    '76000': 116149,
    '6200': 342669,
    '6130': 50396
}

# Appliquer les valeurs de 'p24_pop' pour les codes postaux spécifiés
for code_postal, valeur_p24_pop in valeurs_a_inserer.items():
    final_merged_df.loc[final_merged_df['Postal_Code'] == code_postal, 'p24_pop'] = valeur_p24_pop

# --------------------
# Ajustement final des valeurs pour Paris, Marseille et Lyon
# --------------------

# Données réelles fournies
city_data_real = {
    'Paris': {
        'Superficie': 105.4,
        'Altitude Moyenne': 35,  # Altitude moyenne
        'Population': 2165423  # Population (2021)
    },
    'Marseille': {
        'Superficie': 240.62,
        'Altitude Moyenne': 38,
        'Population': 870018  # Population (2021)
    },
    'Lyon': {
        'Superficie': 47.87,
        'Altitude Moyenne': 170,
        'Population': 522228  # Population (2021)
    }
}

# Mettre à jour les données pour Paris, Marseille et Lyon dans final_merged_df
for city, data in city_data_real.items():
    # Condition pour sélectionner la ville
    city_condition = final_merged_df['City'].str.lower() == city.lower()
    # Mettre à jour les valeurs
    final_merged_df.loc[city_condition, 'Superficie'] = data['Superficie']
    final_merged_df.loc[city_condition, 'Altitude Moyenne'] = data['Altitude Moyenne']
    final_merged_df.loc[city_condition, 'p24_pop'] = data['Population']

columns_to_remove = [
    'DÃÂ©partement', 'RÃÂ©gion', 'Code DÃÂ©partement', 'Code RÃÂ©gion', 'id',
    'nom_commune_postal', 'libelle_acheminement', 'ligne_5', 'latitude', 'longitude', 'code_commune',
    'article', 'nom_commune_complet', 'code_departement', 'presentation', 'slug'
]
final_merged_df.drop(columns=columns_to_remove, inplace=True, errors='ignore')


# Vérifier les valeurs manquantes après la fusion
missing_values_after_merge = final_merged_df.isnull().sum()
print("\nNombre de valeurs manquantes après ajustement final :")
print(missing_values_after_merge)

# Aperçu des premières lignes après la fusion
print("\nAperçu des données finales après ajustements :")
print(final_merged_df.head())

# Exporter le fichier final fusionné
output_file = f"{data_path}villes_pollution_population(FINAL).csv"
final_merged_df.to_csv(output_file, index=False)
print(f"Fichier final exporté sous : {output_file}")

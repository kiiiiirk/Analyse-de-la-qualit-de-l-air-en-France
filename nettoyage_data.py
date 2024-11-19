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
output_file = f"{data_path}population_p1.csv"
merged_df.to_csv(output_file, index=False)  # index=False pour ne pas inclure l'index dans le fichier CSV
print(f"Fichier fusionné exporté sous : {output_file}")


# Charger les fichiers CSV
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

# Fusionner les informations non pivotées avant le pivotement
non_pivot_columns = ['Country.Code', 'Location', 'Latitude', 'Longitude', 'Country.Label', 'Department', 'Region']

# Créer une table contenant uniquement les colonnes non pivotées avec les mêmes index (Postal_Code, City, LastUpdated)
non_pivot_data = villes_polluants[non_pivot_columns + ['Postal_Code', 'City', 'LastUpdated']].drop_duplicates(
    subset=['Postal_Code', 'City', 'LastUpdated'])

# Fusionner les données non pivotées avec la table pivotée
table_pivot = pd.merge(table_pivot, non_pivot_data, on=['Postal_Code', 'City', 'LastUpdated'], how='left')

# Trier par 'Postal_Code', 'City' et 'LastUpdated'
table_pivot.sort_values(by=['Postal_Code', 'City', 'LastUpdated'], inplace=True)

# Mise en index pour l'interpolation
table_pivot.set_index('LastUpdated', inplace=True)

# Appliquer l'interpolation par groupe avec Postal_Code, City, Department et LastUpdated
def interpolate_group(group):
    # Filtrer les colonnes numériques
    numeric_cols = group.select_dtypes(include=['float', 'int']).columns
    # Appliquer l'interpolation temporelle
    group[numeric_cols] = group[numeric_cols].interpolate(method='time', limit_direction='both')
    return group

# Appliquer l'interpolation sur les groupes combinés (Postal_Code, City, Department, LastUpdated)
table_pivot = table_pivot.groupby(['Postal_Code', 'City', 'Department'], group_keys=False).apply(interpolate_group)

# Réinitialiser l'index après interpolation
table_pivot.reset_index(inplace=True)

# Supprimer les colonnes 'CO' et 'NO'
table_pivot.drop(columns=['CO', 'NO'], inplace=True, errors='ignore')
print("Colonnes 'CO' et 'NO' supprimées.")

# Supprimer les colonnes inutiles après la fusion
columns_to_drop = ['Commune', 'DÃ©partement', 'RÃ©gion', 'Statut', 'Population', 'geo_point_2d', 'geo_shape',
                   'ID Geofla', 'Code Commune', 'Code Canton', 'Code Arrondissement', 'Code DÃ©partement', 'Code RÃ©gion',
                   'objectid', 'reg', 'dep', 'cv', 'libgeo', 'p13_pop', 'p14_pop', 'p15_pop']
table_pivot.drop(columns=columns_to_drop, inplace=True, errors='ignore')


# Aperçu de la table après traitement
print("\nAperçu des données après interpolation et fusion :")
print(table_pivot.head())

# Exporter les résultats
output_file = f"{data_path}villes_polluants_cleaned.csv"
table_pivot.to_csv(output_file, index=False)
print(f"Fichier nettoyé exporté sous : {output_file}")



import pandas as pd

# Définir le chemin vers le dossier "données"
data_path = "./données/"  # Ajustez ce chemin si nécessaire

# Charger les fichiers CSV
df_villes_polluants = pd.read_csv(f"{data_path}villes_polluants_cleaned.csv")
df_population = pd.read_csv(f"{data_path}population_p1.csv", encoding='ISO-8859-1')

# Nettoyer les noms des colonnes pour enlever les espaces et caractères invisibles
df_villes_polluants.columns = df_villes_polluants.columns.str.strip()
df_population.columns = df_population.columns.str.strip()

# Préparer les noms des villes pour la fusion
df_villes_polluants['City_short'] = df_villes_polluants['City'].str[:5].str.lower()  # Récupérer les 5 premiers caractères et mettre en minuscule
df_population['Commune_short'] = df_population['Commune'].str[:5].str.lower()  # Récupérer les 5 premiers caractères et mettre en minuscule

# Fonction pour vérifier et corriger les codes postaux si plusieurs sont présents dans la table population
def fix_postal_code(row, villes_polluants):
    # Si le code postal contient un "/"
    if '/' in str(row['Code Postal']):
        # Extraire tous les codes postaux séparés par "/"
        postal_codes = row['Code Postal'].split('/')
        # On vérifie pour chaque code postal séparé par "/" s'il correspond à un code postal dans villes_polluants
        for postal_code in postal_codes:
            postal_code = postal_code.strip()  # Supprimer les espaces inutiles
            # Vérifier si l'un des codes postaux de population correspond à un code postal dans villes_polluants
            if postal_code in villes_polluants['Postal_Code'].values:
                return postal_code
    # Si aucun "/" n'est trouvé, retourner le code postal sans modification
    return row['Code Postal']

# Appliquer la fonction de correction des codes postaux dans la table population
df_population['Code Postal'] = df_population.apply(fix_postal_code, villes_polluants=df_villes_polluants, axis=1)

# Fusionner les deux tables en utilisant Postal_Code et les 5 premiers caractères des noms de villes (City et Commune)
merged_df = pd.merge(df_villes_polluants, df_population, left_on=['Postal_Code', 'City_short'], right_on=['Code Postal', 'Commune_short'], how='inner')

# Supprimer les colonnes inutiles après la fusion
columns_to_drop = ['City_short', 'Commune', 'Commune_short', 'Code Postal', 'Code INSEE', 'Country.Code','Location']
merged_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

# Aperçu des premières lignes après la fusion et suppression des colonnes
print("\nAperçu des données fusionnées après suppression des colonnes inutiles :")
print(merged_df.head())

# Exporter le fichier fusionné
output_file = f"{data_path}villes_population_1.csv.csv"
merged_df.to_csv(output_file, index=False)
print(f"Fichier fusionné exporté sous : {output_file}")


import pandas as pd

# Définir le chemin vers le dossier "données"
data_path = "./données/"  # Ajustez ce chemin si nécessaire

# Charger les fichiers CSV
df_merged_villes_polluants_population = pd.read_csv(f"{data_path}villes_population_1.csv.csv")
df_population_2024 = pd.read_csv(f"{data_path}population_p2.csv", encoding='ISO-8859-1')

# Nettoyer les noms des colonnes pour enlever les espaces et caractères invisibles
df_merged_villes_polluants_population.columns = df_merged_villes_polluants_population.columns.str.strip()
df_population_2024.columns = df_population_2024.columns.str.strip()

# Conserver uniquement la colonne 'population' et renommer en 'p24_pop'
df_population_2024 = df_population_2024[['code_postal', 'population']].rename(columns={'population': 'p24_pop'})

# Supprimer les lignes où la colonne 'p24_pop' contient des valeurs manquantes
df_population_2024.dropna(subset=['p24_pop'], inplace=True)

# Filtrer les lignes de population_2024 qui ont un code postal correspondant à ceux dans merged_villes_polluants_population
df_population_2024_filtered = df_population_2024[df_population_2024['code_postal'].isin(df_merged_villes_polluants_population['Postal_Code'])]

# Réduire les doublons dans df_population_2024_filtered pour n'avoir qu'un seul enregistrement par code postal
df_population_2024_filtered = df_population_2024_filtered.drop_duplicates(subset='code_postal')

# Effectuer une fusion de type left join pour garder toutes les villes dans 'df_merged_villes_polluants_population'
final_merged_df = pd.merge(df_merged_villes_polluants_population, df_population_2024_filtered, left_on='Postal_Code', right_on='code_postal', how='left')

# Supprimer la colonne 'code_postal' du DataFrame final après la fusion
final_merged_df.drop(columns=['code_postal'], inplace=True)

# Modification des valeurs de p24_pop après le merge

# Définir un dictionnaire avec les codes postaux et les valeurs correspondantes à insérer dans p24_pop
valeurs_a_inserer = {
    13200: 52729,
    20000: 71361,
    31100: 498003,
    33000: 268138,
    34000: 315336,
    35000: 221272,
    37100: 137607,
    38000: 158198,
    42000: 171924,
    44000: 333987,
    50100: 79144,
    54000: 104885,
    57000: 122696,
    59000: 238381,
    59240: 85751,
    63000: 144751,
    66000: 122791,
    67000: 290576,
    68100: 108312,
    73440: 2800,
    74190: 11500,
    93200: 115315,
    87000: 127823,
    84000: 89519,
    80000: 134026,
    76600: 163087,
    76200: 27599,
    76000: 116149,
    6200 : 342669,
    6130 : 50396
}

# Parcourir chaque ville dans le dictionnaire et insérer la valeur dans p24_pop
for code_postal, valeur_p24_pop in valeurs_a_inserer.items():
    final_merged_df.loc[final_merged_df['Postal_Code'] == code_postal, 'p24_pop'] = valeur_p24_pop

# Vérifier le résultat (facultatif)
print("\nAprès modification des valeurs p24_pop :")
print(final_merged_df[final_merged_df['Postal_Code'].isin(valeurs_a_inserer.keys())])

# Vérification du nombre de valeurs manquantes après la fusion
missing_values_after_merge = final_merged_df.isnull().sum()

# Afficher le nombre de valeurs manquantes après la fusion
print("\nNombre de valeurs manquantes après fusion :")
print(missing_values_after_merge)

# Aperçu des premières lignes après la fusion
print("\nAperçu des données fusionnées :")
print(final_merged_df.head())

# Exporter le fichier fusionné
output_file = f"{data_path}villes_pollution_population(FINAL).csv"
final_merged_df.to_csv(output_file, index=False)
print(f"Fichier fusionné exporté sous : {output_file}")


import pandas as pd
import logging

# Configuration de la journalisation
logging.basicConfig(
    filename='nettoyage_data.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def load_csv(file_path, encoding='utf-8-sig', delimiter=None, dtype=None, low_memory=True, on_bad_lines='skip'):
    """
    Charge un fichier CSV en utilisant pandas avec une gestion robuste des erreurs.

    Parameters
    ----------
    file_path : str
        Chemin vers le fichier CSV à charger.
    encoding : str, optional
        Encodage du fichier CSV (par défaut 'utf-8-sig').
    delimiter : str, optional
        Délimiteur utilisé dans le fichier CSV (par défaut None, pandas détecte automatiquement).
    dtype : dict, optional
        Dictionnaire spécifiant les types de données pour les colonnes.
    low_memory : bool, optional
        Si False, traite le fichier en une seule passe. (par défaut True)
    on_bad_lines : str, optional
        Comment gérer les lignes incorrectes ('error', 'warn', 'skip'). (par défaut 'skip')

    Returns
    -------
    pd.DataFrame
        DataFrame contenant les données chargées.
    """
    try:
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            delimiter=delimiter,
            dtype=dtype,
            low_memory=low_memory,
            on_bad_lines=on_bad_lines
        )
        logging.info(f"Chargé avec succès : {file_path}")
        return df
    except pd.errors.ParserError as e:
        logging.error(f"Erreur de parsing lors du chargement de {file_path} : {e}")
        raise
    except Exception as e:
        logging.error(f"Erreur inattendue lors du chargement de {file_path} : {e}")
        raise


def clean_column_names(df):
    """
    Nettoie les noms des colonnes d'un DataFrame en supprimant les espaces et caractères invisibles.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame dont les noms des colonnes doivent être nettoyés.

    Returns
    -------
    pd.DataFrame
        DataFrame avec des noms de colonnes nettoyés.
    """
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace('ï»¿', '', regex=False)
    logging.info(f"Noms des colonnes nettoyés. Avant: {original_columns}, Après: {df.columns.tolist()}")
    return df


def rename_columns_population(df):
    """
    Renomme les colonnes du DataFrame de population pour correspondre à celles de correspondance.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame de population dont les colonnes doivent être renommées.

    Returns
    -------
    pd.DataFrame
        DataFrame avec les colonnes renommées.
    """
    rename_dict = {
        'code_commune_INSEE': 'Code INSEE',
        'code_postal': 'Code Postal',
        'nom_commune': 'Commune',
        'surface': 'Superficie',
        'population': 'Population'
    }
    df.rename(columns=rename_dict, inplace=True)
    logging.info(f"Colonnes renommées: {rename_dict}")
    return df


def convert_columns_to_str(df, columns):
    """
    Convertit les colonnes spécifiées en type chaîne de caractères.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame contenant les colonnes à convertir.
    columns : list of str
        Liste des noms des colonnes à convertir.

    Returns
    -------
    pd.DataFrame
        DataFrame avec les colonnes converties.
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
            logging.info(f"Colonne convertie en str: {col}")
    return df


def drop_duplicates_and_na(df, subset_columns):
    """
    Supprime les doublons et les lignes avec des valeurs manquantes dans les colonnes spécifiées.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame à nettoyer.
    subset_columns : list of str
        Colonnes à utiliser pour identifier les doublons et les valeurs manquantes.

    Returns
    -------
    pd.DataFrame
        DataFrame nettoyé.
    """
    initial_shape = df.shape
    df.drop_duplicates(subset=subset_columns, keep='first', inplace=True)
    df.dropna(subset=subset_columns, inplace=True)
    final_shape = df.shape
    logging.info(f"Duplication et NA supprimés. Initial: {initial_shape}, Final: {final_shape}")
    return df


def merge_dataframes(df1, df2, on_columns, suffixes=('_correspondance', '_population')):
    """
    Fusionne deux DataFrames sur les colonnes spécifiées.

    Parameters
    ----------
    df1 : pd.DataFrame
        Premier DataFrame à fusionner.
    df2 : pd.DataFrame
        Deuxième DataFrame à fusionner.
    on_columns : list of str
        Colonnes sur lesquelles la fusion doit être effectuée.
    suffixes : tuple of str, optional
        Suffixes à ajouter aux colonnes en cas de chevauchement (par défaut ('_correspondance', '_population')).

    Returns
    -------
    pd.DataFrame
        DataFrame fusionné.
    """
    merged_df = pd.merge(df1, df2, on=on_columns, how='inner', suffixes=suffixes)
    logging.info(f"Fusion effectuée sur les colonnes: {on_columns}")
    return merged_df


def clean_and_export_population_data(data_path):
    """
    Étape 1 : Traitement des données de population.

    Parameters
    ----------
    data_path : str
        Chemin vers le dossier contenant les fichiers de données.
    """
    try:
        # Charger les fichiers CSV avec low_memory=False pour éviter les DtypeWarning
        df_correspondance = load_csv(f"{data_path}correspondance-code-insee-code-postal.csv", delimiter=';',
                                     low_memory=False)
        df_population_p2 = load_csv(f"{data_path}population_p2.csv", low_memory=False)

        # Nettoyer les noms des colonnes
        df_correspondance = clean_column_names(df_correspondance)
        df_population_p2 = clean_column_names(df_population_p2)

        # Renommer les colonnes dans df_population_p2
        df_population_p2 = rename_columns_population(df_population_p2)

        # Vérifier la présence de la colonne 'Code INSEE'
        if 'Code INSEE' not in df_population_p2.columns:
            raise KeyError("La colonne 'Code INSEE' n'existe pas dans df_population_p2. Vérifiez le nom de la colonne.")

        # Conversion des types de données pour les colonnes de fusion
        columns_to_convert = ['Code INSEE', 'Code Postal']
        df_correspondance = convert_columns_to_str(df_correspondance, columns_to_convert)
        df_population_p2 = convert_columns_to_str(df_population_p2, columns_to_convert)

        # Supprimer les doublons et les valeurs manquantes
        subset_columns_corresp = ['Code INSEE', 'Code Postal']
        df_correspondance = drop_duplicates_and_na(df_correspondance, subset_columns_corresp + ['Altitude Moyenne'])
        df_population_p2 = drop_duplicates_and_na(df_population_p2,
                                                  subset_columns_corresp + ['Population', 'Superficie'])

        # Fusionner les deux DataFrames
        merged_df = merge_dataframes(df_correspondance, df_population_p2, on_columns=subset_columns_corresp)

        # Conserver la colonne 'Commune' après la fusion
        if 'Commune_population' in merged_df.columns:
            merged_df['Commune'] = merged_df['Commune_population']
            logging.info("Colonne 'Commune_population' utilisée pour 'Commune'.")
        elif 'Commune_correspondance' in merged_df.columns:
            merged_df['Commune'] = merged_df['Commune_correspondance']
            logging.info("Colonne 'Commune_correspondance' utilisée pour 'Commune'.")
        else:
            logging.warning("Aucune colonne 'Commune' trouvée après fusion.")

        # Supprimer les colonnes non nécessaires après la fusion
        columns_to_drop = [
            'Statut', 'geo_point_2d', 'geo_shape', 'ID Geofla',
            'Code Commune', 'Code Canton', 'Code Arrondissement',
            'Population_correspondance', 'Population_population',
            'Superficie_correspondance', 'Superficie_population',
            'Commune_correspondance', 'Commune_population'
        ]
        merged_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
        logging.info(f"Colonnes supprimées après fusion: {columns_to_drop}")

        # Supprimer les colonnes spécifiées par l'utilisateur
        columns_to_remove = [
            'Département', 'Région', 'Code Département', 'Code Région', 'id',
            'nom_commune_postal', 'libelle_acheminement', 'ligne_5', 'latitude',
            'longitude', 'code_commune', 'article', 'nom_commune_complet',
            'code_departement', 'presentation', 'slug'
        ]
        # Corriger les noms de colonnes mal encodés si nécessaire
        corrected_columns_to_remove = [col.encode('latin1').decode('utf-8') if 'Ã' in col else col for col in
                                       columns_to_remove]
        merged_df.drop(columns=corrected_columns_to_remove, inplace=True, errors='ignore')
        logging.info(f"Colonnes spécifiées par l'utilisateur supprimées: {corrected_columns_to_remove}")

        # Exporter le fichier fusionné en CSV
        output_file = f"{data_path}population_p1.csv"
        merged_df.to_csv(output_file, index=False)
        logging.info(f"Fichier fusionné exporté sous : {output_file}")
        print(f"Fichier fusionné exporté sous : {output_file}")
    except Exception as e:
        logging.error(f"Erreur dans clean_and_export_population_data: {e}")
        raise


def process_pollutant_data(data_path):
    """
    Étape 2 : Traitement des données de polluants.

    Parameters
    ----------
    data_path : str
        Chemin vers le dossier contenant les fichiers de données.
    """
    try:
        # Charger le fichier CSV des polluants
        villes_polluants = load_csv(f"{data_path}villes_polluants.csv", low_memory=False)

        # Convertir la colonne 'LastUpdated' en datetime
        villes_polluants['LastUpdated'] = pd.to_datetime(villes_polluants['LastUpdated'], errors='coerce')
        logging.info("Colonne 'LastUpdated' convertie en datetime.")

        # Vérifier les valeurs manquantes dans 'Postal_Code'
        if villes_polluants['Postal_Code'].isnull().any():
            raise ValueError(
                "Certaines lignes ne contiennent pas de code postal. Veuillez nettoyer les données avant de continuer.")
        logging.info("Aucune valeur manquante dans 'Postal_Code'.")

        # Pivoter la table pour avoir les polluants en colonnes
        table_pivot = villes_polluants.pivot_table(
            index=['Postal_Code', 'City', 'LastUpdated'],
            columns='Pollutant',
            values='value',
            aggfunc='mean'
        )
        logging.info("Table pivotée pour les polluants.")

        # Réinitialiser l'index pour revenir à une structure tabulaire
        table_pivot.reset_index(inplace=True)

        # Fusionner les informations non pivotées avant le pivotement
        non_pivot_columns = ['Country.Code', 'Location', 'Latitude', 'Longitude', 'Country.Label', 'Department',
                             'Region']
        non_pivot_data = villes_polluants[non_pivot_columns + ['Postal_Code', 'City', 'LastUpdated']].drop_duplicates(
            subset=['Postal_Code', 'City', 'LastUpdated']
        )
        table_pivot = pd.merge(table_pivot, non_pivot_data, on=['Postal_Code', 'City', 'LastUpdated'], how='left')
        logging.info("Informations non pivotées fusionnées avec la table pivotée.")

        # Trier les données
        table_pivot.sort_values(by=['Postal_Code', 'City', 'LastUpdated'], inplace=True)
        logging.info("Données triées par 'Postal_Code', 'City', 'LastUpdated'.")

        # Mise en index pour l'interpolation
        table_pivot.set_index('LastUpdated', inplace=True)

        # Fonction d'interpolation par groupe
        def interpolate_group(group):
            numeric_cols = group.select_dtypes(include=['float', 'int']).columns
            group[numeric_cols] = group[numeric_cols].interpolate(method='time', limit_direction='both')
            return group

        # Identifier les colonnes numériques à interpoler
        numeric_cols = table_pivot.select_dtypes(include=['float', 'int']).columns

        # Appliquer l'interpolation sur les colonnes numériques groupées par 'Postal_Code', 'City', 'Department'
        table_pivot[numeric_cols] = table_pivot.groupby(['Postal_Code', 'City', 'Department'])[numeric_cols].transform(
            lambda x: x.interpolate(method='time', limit_direction='both')
        )
        logging.info("Interpolation temporelle appliquée aux groupes.")

        # Réinitialiser l'index après interpolation
        table_pivot.reset_index(inplace=True)

        # Supprimer les colonnes 'CO' et 'NO' si elles existent
        table_pivot.drop(columns=['CO', 'NO'], inplace=True, errors='ignore')
        logging.info("Colonnes 'CO' et 'NO' supprimées.")
        print("Colonnes 'CO' et 'NO' supprimées.")

        # Exporter les résultats
        output_file = f"{data_path}villes_polluants_cleaned.csv"
        table_pivot.to_csv(output_file, index=False)
        logging.info(f"Fichier nettoyé exporté sous : {output_file}")
        print(f"Fichier nettoyé exporté sous : {output_file}")
    except Exception as e:
        logging.error(f"Erreur dans process_pollutant_data: {e}")
        raise


def merge_pollution_and_population(data_path):
    """
    Étape 3 : Fusion des données de pollution et de population.

    Parameters
    ----------
    data_path : str
        Chemin vers le dossier contenant les fichiers de données.
    """
    try:
        # Charger les fichiers CSV fusionnés précédemment
        df_villes_polluants = load_csv(f"{data_path}villes_polluants_cleaned.csv")
        df_population = load_csv(f"{data_path}population_p1.csv", low_memory=False)

        # Nettoyer les noms des colonnes
        df_villes_polluants = clean_column_names(df_villes_polluants)
        df_population = clean_column_names(df_population)

        # Vérifier la présence de la colonne 'Commune'
        if 'Commune' not in df_population.columns:
            raise KeyError("La colonne 'Commune' est manquante dans df_population. Vérifiez la fusion précédente.")
        logging.info("Colonne 'Commune' présente dans df_population.")

        # Préparer les noms des villes pour la fusion
        df_villes_polluants['City_short'] = df_villes_polluants['City'].str[:5].str.lower()
        df_population['Commune_short'] = df_population['Commune'].str[:5].str.lower()
        logging.info("Noms des villes préparés pour la fusion.")

        # Fonction pour vérifier et corriger les codes postaux
        def fix_postal_code(row, villes_polluants):
            """
            Corrige les codes postaux contenant plusieurs valeurs séparées par '/'.

            Parameters
            ----------
            row : pd.Series
                Ligne du DataFrame population.
            villes_polluants : pd.DataFrame
                DataFrame contenant les codes postaux valides.

            Returns
            -------
            str
                Code postal corrigé.
            """
            if '/' in str(row['Code Postal']):
                postal_codes = row['Code Postal'].split('/')
                for postal_code in postal_codes:
                    postal_code = postal_code.strip()
                    if postal_code in villes_polluants['Postal_Code'].astype(str).values:
                        return postal_code
            return row['Code Postal']

        # Appliquer la correction des codes postaux
        df_population['Code Postal'] = df_population.apply(
            fix_postal_code,
            villes_polluants=df_villes_polluants,
            axis=1
        )
        logging.info("Codes postaux corrigés dans df_population.")

        # Convertir les codes postaux en chaînes de caractères
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
        logging.info("Fusion des données de pollution et de population effectuée.")

        # Supprimer les colonnes inutiles après la fusion
        columns_to_drop = [
            'City_short', 'Commune', 'Commune_short', 'Code Postal', 'Code INSEE',
            'Country.Code', 'Location'
        ]
        merged_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
        logging.info(f"Colonnes supprimées après fusion: {columns_to_drop}")

        # Supprimer les colonnes spécifiées par l'utilisateur avec correction d'encodage
        columns_to_remove = [
            'Département', 'Région', 'Code Département', 'Code Région', 'id',
            'nom_commune_postal', 'libelle_acheminement', 'ligne_5', 'latitude',
            'longitude', 'code_commune', 'article', 'nom_commune_complet',
            'code_departement', 'presentation', 'slug'
        ]
        # Corriger les noms de colonnes mal encodés si nécessaire
        corrected_columns_to_remove = [col.encode('latin1').decode('utf-8') if 'Ã' in col else col for col in
                                       columns_to_remove]
        merged_df.drop(columns=corrected_columns_to_remove, inplace=True, errors='ignore')
        logging.info(f"Colonnes spécifiées par l'utilisateur supprimées: {corrected_columns_to_remove}")

        # Exporter le fichier fusionné
        output_file = f"{data_path}villes_population_1.csv"
        merged_df.to_csv(output_file, index=False)
        logging.info(f"Fichier fusionné exporté sous : {output_file}")
        print(f"Fichier fusionné exporté sous : {output_file}")
    except Exception as e:
        logging.error(f"Erreur dans merge_pollution_and_population: {e}")
        raise


def final_population_processing(data_path):
    """
    Étape 4 : Traitement final avec les données de population de 2024.

    Parameters
    ----------
    data_path : str
        Chemin vers le dossier contenant les fichiers de données.
    """
    try:
        # Charger les fichiers CSV
        df_merged = load_csv(f"{data_path}villes_population_1.csv")

        # Déterminer le délimiteur correct pour population_p2.csv
        # Supposons qu'il s'agit d'une virgule, sinon ajustez en conséquence
        df_population_2024 = load_csv(f"{data_path}population_p2.csv", low_memory=False, on_bad_lines='skip')
        logging.info("Fichier 'population_p2.csv' chargé avec succès.")

        # Nettoyer les noms des colonnes
        df_merged = clean_column_names(df_merged)
        df_population_2024 = clean_column_names(df_population_2024)

        # Renommer les colonnes dans df_population_2024
        rename_dict = {
            'code_postal': 'Postal_Code',
            'population': 'p24_pop',
            'surface': 'Superficie',
            'nom_commune': 'Commune'
        }
        df_population_2024.rename(columns=rename_dict, inplace=True)
        logging.info(f"Colonnes renommées dans df_population_2024: {rename_dict}")

        # Supprimer les arrondissements des villes cibles (Paris, Marseille, Lyon) sauf la ville elle-même
        df_population_2024 = df_population_2024[
            ~df_population_2024['Commune'].str.contains(
                r'^(?:Paris|Marseille|Lyon) [0-9]+(?:er|ème|e)?$',
                na=False
            )
        ]
        logging.info("Arrondissements des villes cibles supprimés.")

        # Supprimer les lignes où 'p24_pop' ou 'Postal_Code' est manquant
        df_population_2024.dropna(subset=['p24_pop', 'Postal_Code'], inplace=True)
        logging.info("Lignes avec 'p24_pop' ou 'Postal_Code' manquants supprimées.")

        # Convertir 'Postal_Code' en chaîne de caractères
        df_population_2024['Postal_Code'] = df_population_2024['Postal_Code'].astype(str)
        df_merged['Postal_Code'] = df_merged['Postal_Code'].astype(str)
        logging.info("'Postal_Code' converti en chaîne de caractères.")

        # Filtrer les lignes de df_population_2024 qui ont un code postal correspondant
        df_population_2024_filtered = df_population_2024[
            df_population_2024['Postal_Code'].isin(df_merged['Postal_Code'])
        ]
        logging.info("Filtrage des codes postaux correspondants effectué.")

        # Réduire les doublons pour n'avoir qu'un enregistrement par code postal
        df_population_2024_filtered = df_population_2024_filtered.drop_duplicates(subset='Postal_Code')
        logging.info("Doublons réduits dans df_population_2024_filtered.")

        # Fusionner avec df_merged_villes_polluants_population sur 'Postal_Code'
        final_merged_df = pd.merge(
            df_merged,
            df_population_2024_filtered[['Postal_Code', 'p24_pop', 'Superficie']],
            on='Postal_Code',
            how='left'
        )
        logging.info("Fusion finale effectuée sur 'Postal_Code'.")

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
            logging.info(f"Valeur 'p24_pop' mise à jour pour le code postal {code_postal}.")

        # Ajustement final des valeurs pour Paris, Marseille et Lyon
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
            logging.info(f"Données mises à jour pour {city}.")

        # Supprimer les colonnes inutiles après ajustements avec correction d'encodage
        columns_to_remove = [
            'Département', 'Région', 'Code Département', 'Code Région', 'id',
            'nom_commune_postal', 'libelle_acheminement', 'ligne_5', 'latitude',
            'longitude', 'code_commune', 'article', 'nom_commune_complet',
            'code_departement', 'presentation', 'slug'
        ]
        # Corriger les noms de colonnes mal encodés si nécessaire
        corrected_columns_to_remove = [col.encode('latin1').decode('utf-8') if 'Ã' in col else col for col in
                                       columns_to_remove]
        final_merged_df.drop(columns=corrected_columns_to_remove, inplace=True, errors='ignore')
        logging.info(f"Colonnes inutiles supprimées: {corrected_columns_to_remove}")

        # Vérifier les valeurs manquantes après la fusion
        missing_values_after_merge = final_merged_df.isnull().sum()
        logging.info(f"Valeurs manquantes après fusion:\n{missing_values_after_merge}")
        print("\nNombre de valeurs manquantes après ajustement final :")
        print(missing_values_after_merge)

        # Aperçu des premières lignes après la fusion
        print("\nAperçu des données finales après ajustements :")
        print(final_merged_df.head())
        logging.info("Aperçu des données finales affiché.")

        # Exporter le fichier final fusionné
        output_file = f"{data_path}villes_pollution_population(FINAL).csv"
        final_merged_df.to_csv(output_file, index=False)
        logging.info(f"Fichier final exporté sous : {output_file}")
        print(f"Fichier final exporté sous : {output_file}")
    except Exception as e:
        logging.error(f"Erreur dans final_population_processing: {e}")
        raise


def main():
    """
    Fonction principale pour exécuter toutes les étapes de traitement des données.
    """
    try:
        # Définir le chemin vers le dossier "données"
        data_path = "./données/"  # Ajustez ce chemin si nécessaire

        # Étape 1 : Traitement des données de population
        print("Étape 1 : Traitement des données de population")
        logging.info("Début de l'étape 1 : Traitement des données de population.")
        clean_and_export_population_data(data_path)

        # Étape 2 : Traitement des données de polluants
        print("\nÉtape 2 : Traitement des données de polluants")
        logging.info("Début de l'étape 2 : Traitement des données de polluants.")
        process_pollutant_data(data_path)

        # Étape 3 : Fusion des données de pollution et de population
        print("\nÉtape 3 : Fusion des données de pollution et de population")
        logging.info("Début de l'étape 3 : Fusion des données de pollution et de population.")
        merge_pollution_and_population(data_path)

        # Étape 4 : Traitement final avec les données de population de 2024
        print("\nÉtape 4 : Traitement final avec les données de population de 2024")
        logging.info("Début de l'étape 4 : Traitement final avec les données de population de 2024.")
        final_population_processing(data_path)

    except Exception as e:
        logging.critical(f"Erreur critique dans main: {e}")
        print(f"Une erreur critique est survenue : {e}")


if __name__ == "__main__":
    main()

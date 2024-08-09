# from pyspark.sql import functions as F
# from pyspark.ml.feature import StringIndexer
# from pyspark.ml import Pipeline
# from pyspark.sql import SparkSession

# def use_planets(df,pre_fitted_pipeline):
#     # Drop unnecessary columns
#     compact_data = df.drop('P_GEO_ALBEDO', 'P_DETECTION_MASS', 'P_DETECTION_RADIUS', 'P_ALT_NAMES', 'P_ATMOSPHERE',
#                            'S_DISC', 'S_MAGNETIC_FIELD', 'P_TEMP_MEASURED', 'P_TPERI', 'P_DENSITY', 'P_ESCAPE', 
#                            'P_GRAVITY', 'P_POTENTIAL', 'P_OMEGA', 'P_INCLINATION', 'P_IMPACT_PARAMETER', 
#                            'P_HILL_SPHERE', 'P_UPDATE', 'P_MASS')

#     # Drop error columns
#     error_columns = [col for col in compact_data.columns if 'ERROR' in col]
#     df_cleaned = compact_data.drop(*error_columns)

#     # Fill missing values with a constant, for example, 0
#     compact_data = df_cleaned.na.fill(0)

#     # Apply the pre-fitted pipeline directly
#     compact_data = pre_fitted_pipeline.transform(compact_data)

#     # Further cleanup
#     cols_to_remove = list(set(['S_HZ_OPT_MIN_imputed', 'P_FLUX_MAX', 'P_TEMP_EQUIL_imputed', 'S_NAME_HD_indexed_imputed', 
#                                'P_MASS_LIMIT_imputed', 'P_FLUX_MIN', 'S_LOG_G_LIMIT', 'S_SNOW_LINE_imputed', 
#                                'P_HABZONE_CON_imputed', 'S_NAME_HIP_indexed_imputed', 'P_TEMP_EQUIL_MAX', 
#                                'S_HZ_CON_MIN_imputed', 'P_APASTRON_imputed', 'P_SEMI_MAJOR_AXIS_LIMIT_imputed', 
#                                'S_HZ_CON0_MIN_imputed', 'S_TYPE_indexed_imputed', 'S_HZ_CON_MAX', 'S_TIDAL_LOCK_imputed', 
#                                'P_INCLINATION_LIMIT_imputed', 'P_PERIASTRON_imputed', 'S_DEC_imputed', 
#                                'P_ECCENTRICITY_LIMIT_imputed', 'S_METALLICITY_imputed', 'S_DEC_TXT_indexed', 
#                                'S_ABIO_ZONE_imputed', 'S_HZ_CON1_MAX', 'P_DISTANCE_EFF_imputed', 
#                                'P_TEMP_SURF_MAX_imputed', 'S_TYPE_TEMP_indexed_imputed', 'S_MAG_imputed', 
#                                'S_HZ_OPT_MAX', 'S_CONSTELLATION_ENG_indexed_imputed', 'P_ECCENTRICITY_imputed', 
#                                'S_HZ_CON1_MIN', 'S_SNOW_LINE', 'S_METALLICITY_LIMIT_imputed', 'S_RA_TXT_indexed', 
#                                'P_TEMP_EQUIL_MIN', 'P_HABZONE_OPT_imputed', 'S_CONSTELLATION_ABR_indexed', 
#                                'P_TEMP_EQUIL_MAX_imputed', 'S_AGE_imputed', 'S_HZ_CON0_MAX_imputed', 
#                                'P_ESI_imputed', 'S_RA_imputed', 'S_HZ_CON_MIN', 'S_LOG_LUM_imputed', 
#                                'S_CONSTELLATION_ENG_indexed', 'P_APASTRON', 'P_FLUX_MAX_imputed', 'P_TEMP_SURF_MAX', 
#                                'P_DISTANCE', 'S_RA_TXT_indexed_imputed', 'S_HZ_CON0_MIN', 'P_YEAR_imputed', 
#                                'P_TYPE_indexed_imputed', 'S_LUMINOSITY_imputed', 'P_PERIOD_imputed', 
#                                'S_TEMPERATURE_imputed', 'P_HABITABLE_imputed', 'S_RA_STR_indexed_imputed', 
#                                'S_TEMPERATURE_LIMIT_imputed', 'S_CONSTELLATION_indexed_imputed', 
#                                'S_HZ_CON0_MAX', 'P_DISCOVERY_FACILITY_indexed_imputed', 
#                                'S_HZ_CON1_MIN_imputed', 'S_LOG_G_LIMIT_imputed', 'P_RADIUS_LIMIT_imputed', 
#                                'S_LOG_G_imputed', 'S_AGE_LIMIT_imputed', 'P_NAME_indexed_imputed', 'S_RADIUS_imputed', 
#                                'P_DETECTION_indexed_imputed', 'P_TEMP_SURF_MIN_imputed', 'P_TYPE_TEMP_indexed_imputed', 
#                                'P_PERIOD_LIMIT_imputed', 'P_RADIUS_imputed', 'P_TEMP_EQUIL_MIN_imputed', 
#                                'P_MASS_ORIGIN_indexed_imputed', 'S_HZ_CON_MAX_imputed', 'P_DISTANCE_EFF', 
#                                'S_DEC_TXT_indexed_imputed', 'S_NAME_indexed_imputed', 
#                                'S_CONSTELLATION_ABR_indexed_imputed', 'P_FLUX_MIN_imputed', 'P_PERIASTRON', 
#                                'P_TEMP_SURF_imputed', 'S_DEC_STR_indexed_imputed', 'S_MASS_imputed', 
#                                'S_HZ_OPT_MAX_imputed', 'P_FLUX_imputed', 'P_SEMI_MAJOR_AXIS_imputed', 
#                                'P_DISTANCE_imputed', 'S_DISTANCE_imputed', 'S_HZ_CON1_MAX_imputed', 'P_TEMP_SURF_MIN']))
#     compact_data = compact_data.drop(*cols_to_remove)
    
#     return compact_data

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.types import DateType
from pyspark.ml.feature import StringIndexer, Imputer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def use_planets(df):
    #création session spark
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    #drop colonnes inutiles
    compact_data = df.drop('P_GEO_ALBEDO', 'P_DETECTION_MASS', 'P_DETECTION_RADIUS', 'P_ALT_NAMES', 'P_ATMOSPHERE',
                           'S_DISC', 'S_MAGNETIC_FIELD',
                           'P_TEMP_MEASURED', 'P_TPERI',
                           'P_DENSITY', 'P_ESCAPE', 'P_GRAVITY', 'P_POTENTIAL', 'P_OMEGA', 'P_INCLINATION',
                           'P_IMPACT_PARAMETER', 'P_HILL_SPHERE', 'P_UPDATE',
                           'P_MASS')

    error_columns = [col for col in compact_data.columns if 'ERROR' in col]
    df_cleaned = compact_data.drop(*error_columns)
    #remplir les valeurs manquantes avec la moyenne
    mean_values = {
    col_name: avg_value
    for col_name, avg_value in (
        (col_name, df_cleaned.select(avg(col_name)).first()[0])
        for col_name, dtype in df_cleaned.dtypes
        if dtype != 'string' and not isinstance(df_cleaned.schema[col_name].dataType, DateType)
    )
    if avg_value is not None
}
    compact_data = df_cleaned.na.fill(mean_values)
    # Identifier les colonnes catégorielles
    categorical_columns = [col_name for col_name, dtype in compact_data.dtypes if dtype == 'string']

    # Créer un indexeur pour chaque colonne catégorielle
    indexers = [StringIndexer(inputCol=column, outputCol=column + "_indexed").setHandleInvalid("keep") for column in
                categorical_columns]

    # Créer le pipeline
    pipeline = Pipeline(stages=indexers)

    # Appliquer le pipeline au DataFrame
    compact_data = pipeline.fit(compact_data).transform(compact_data)

    # supprimer les anciennes colonnes catégorielles si nécessaire
    for column in categorical_columns:
        compact_data = compact_data.drop(column)

    # Identifier les colonnes numériques pour l'imputation
    numeric_cols = [col_name for col_name, dtype in compact_data.dtypes if dtype in ['int', 'double']]

    # Créer un imputer pour les colonnes numériques identifiées
    imputer = Imputer(
        inputCols=numeric_cols,
        outputCols=["{}_imputed".format(col_name) for col_name in numeric_cols]
    ).setStrategy("mean")  # ou "median" selon le cas d'usage

    # Appliquer l'imputation
    
    compact_data=compact_data.fillna(0)
#   compact_data = imputer.fit(compact_data).transform(compact_data)
#   # Identifier les colonnes numériques
#   numeric_cols = [col_name for col_name, dtype in compact_data.dtypes if dtype in ['int', 'double']]
#
#   # Assembler les colonnes numériques dans un vecteur
#   assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
#   feature_vector_df = assembler.transform(compact_data)
#
#   # Calculer la matrice de corrélation
#   correlation_matrix = Correlation.corr(feature_vector_df, "features").head()[0]
#
#   # Extraire la matrice de corrélation en tant que tableau Numpy (si nécessaire)
#   correlation_array = correlation_matrix.toArray()
#   # Seuil de corrélation haute pour retirer les colonnes
#   high_corr_threshold = 0.9
#   cols_to_remove = []
#
#   for i in range(len(correlation_array)):
#       for j in range(i + 1, len(correlation_array)):
#           if abs(correlation_array[i, j]) > high_corr_threshold:
#               cols_to_remove.append(numeric_cols[j])
#
#   # Supprimer les colonnes sélectionnées
    cols_to_remove = list(set(['S_HZ_OPT_MIN_imputed', 'P_FLUX_MAX', 'P_TEMP_EQUIL_imputed', 'S_NAME_HD_indexed_imputed', 'P_MASS_LIMIT_imputed', 'P_FLUX_MIN', 'S_LOG_G_LIMIT', 'S_SNOW_LINE_imputed', 'P_HABZONE_CON_imputed', 'S_NAME_HIP_indexed_imputed', 'P_TEMP_EQUIL_MAX', 'S_HZ_CON_MIN_imputed', 'P_APASTRON_imputed', 'P_SEMI_MAJOR_AXIS_LIMIT_imputed', 'S_HZ_CON0_MIN_imputed', 'S_TYPE_indexed_imputed', 'S_HZ_CON_MAX', 'S_TIDAL_LOCK_imputed', 'P_INCLINATION_LIMIT_imputed', 'P_PERIASTRON_imputed', 'S_DEC_imputed', 'P_ECCENTRICITY_LIMIT_imputed', 'S_METALLICITY_imputed', 'S_DEC_TXT_indexed', 'S_ABIO_ZONE_imputed', 'S_HZ_CON1_MAX', 'P_DISTANCE_EFF_imputed', 'P_TEMP_SURF_MAX_imputed', 'S_TYPE_TEMP_indexed_imputed', 'S_MAG_imputed', 'S_HZ_OPT_MAX', 'S_CONSTELLATION_ENG_indexed_imputed', 'P_ECCENTRICITY_imputed', 'S_HZ_CON1_MIN', 'S_SNOW_LINE', 'S_METALLICITY_LIMIT_imputed', 'S_RA_TXT_indexed', 'P_TEMP_EQUIL_MIN', 'P_HABZONE_OPT_imputed', 'S_CONSTELLATION_ABR_indexed', 'P_TEMP_EQUIL_MAX_imputed', 'S_AGE_imputed', 'S_HZ_CON0_MAX_imputed', 'P_ESI_imputed', 'S_RA_imputed', 'S_HZ_CON_MIN', 'S_LOG_LUM_imputed', 'S_CONSTELLATION_ENG_indexed', 'P_APASTRON', 'P_FLUX_MAX_imputed', 'P_TEMP_SURF_MAX', 'P_DISTANCE', 'S_RA_TXT_indexed_imputed', 'S_HZ_CON0_MIN', 'P_YEAR_imputed', 'P_TYPE_indexed_imputed', 'S_LUMINOSITY_imputed', 'P_PERIOD_imputed', 'S_TEMPERATURE_imputed', 'P_HABITABLE_imputed', 'S_RA_STR_indexed_imputed', 'S_TEMPERATURE_LIMIT_imputed', 'S_CONSTELLATION_indexed_imputed', 'S_HZ_CON0_MAX', 'P_DISCOVERY_FACILITY_indexed_imputed', 'S_HZ_CON1_MIN_imputed', 'S_LOG_G_LIMIT_imputed', 'P_RADIUS_LIMIT_imputed', 'S_LOG_G_imputed', 'S_AGE_LIMIT_imputed', 'P_NAME_indexed_imputed', 'S_RADIUS_imputed', 'P_DETECTION_indexed_imputed', 'P_TEMP_SURF_MIN_imputed', 'P_TYPE_TEMP_indexed_imputed', 'P_PERIOD_LIMIT_imputed', 'P_RADIUS_imputed', 'P_TEMP_EQUIL_MIN_imputed', 'P_MASS_ORIGIN_indexed_imputed', 'S_HZ_CON_MAX_imputed', 'P_DISTANCE_EFF', 'S_DEC_TXT_indexed_imputed', 'S_NAME_indexed_imputed', 'S_CONSTELLATION_ABR_indexed_imputed', 'P_FLUX_MIN_imputed', 'P_PERIASTRON', 'P_TEMP_SURF_imputed', 'S_DEC_STR_indexed_imputed', 'S_MASS_imputed', 'S_HZ_OPT_MAX_imputed', 'P_FLUX_imputed', 'P_SEMI_MAJOR_AXIS_imputed', 'P_DISTANCE_imputed', 'S_DISTANCE_imputed', 'S_HZ_CON1_MAX_imputed', 'P_TEMP_SURF_MIN']))  # Enlever les doublons
    compact_data = compact_data.drop(*cols_to_remove)
    return compact_data
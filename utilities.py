from pyspark.sql.functions import *
from pyspark.sql.types import *

# Function to calculate percentage of null values
def null_percentage(df):
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    total_count = df.count()
    return {k: v/total_count*100 for k, v in null_counts.asDict().items()}

def display_null_percentages(null_counts_dict):
  for col, percentage in null_counts_dict.items():
    print(f"{col:30s}{percentage:.2f}%")

def detect_outliers(df):
    outlier_dfs = []
    for col_name in df.schema.names:
        col_type = df.schema[col_name].dataType
        if col_type == IntegerType() or col_type == LongType() or col_type == FloatType() or col_type == DoubleType():
            quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.05)
            IQR = quantiles[1] - quantiles[0]
            lower_bound = quantiles[0] - 1.5 * IQR
            upper_bound = quantiles[1] + 1.5 * IQR
            outlier_df = df.filter((F.col(col_name) < lower_bound) | (F.col(col_name) > upper_bound))
            outlier_dfs.append(outlier_df)
    return outlier_dfs
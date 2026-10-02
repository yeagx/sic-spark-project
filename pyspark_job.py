from pyspark.sql import functions as F


def clean_data(df):
    return (
        df.filter(F.col("amount") > 0)                                # rule 1
          .filter(F.col("name").isNotNull())                          # rule 2
          .withColumn("amount_with_tax", F.col("amount") * 1.20)      # rule 3
    )
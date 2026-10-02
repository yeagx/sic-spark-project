from pyspark.sql import functions as F


def clean_data(df):
    """Drop rows with amount <= 0 or NULL name, then add amount_with_tax (amount * 1.20)."""
    return (
        df.filter(F.col("amount") > 0)                                # rule 1
          .filter(F.col("name").isNotNull())                          # rule 2
          .withColumn("amount_with_tax", F.col("amount") * 1.20)      # rule 3
    )
from pyspark.sql.functions import col

def check_not_null(df, column):
    return df.filter(col(column).isNull()).count() == 0

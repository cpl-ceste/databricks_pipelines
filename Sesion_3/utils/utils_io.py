def save_table(df, catalog, schema, table, mode="overwrite"):
    full_name = f"{catalog}.{schema}.{table}"
    print(full_name)
    df.write.mode(mode).saveAsTable(full_name)

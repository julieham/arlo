def write_df_to_csv(df, filename, sep=';', header=True, index=True):
    df.to_csv(filename, sep=sep, header=header, index=index)

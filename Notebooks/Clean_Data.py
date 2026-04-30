
# Clean All rows which tenure above age
def clean_rows_tenure_above_age(df):
    rows_to_drop = df[df['tenure'] > df['age']].index
    df = df.drop(rows_to_drop)
    return df


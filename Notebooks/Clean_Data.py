import pandas as pd

# Clean All rows which tenure above age
def clean_rows_tenure_above_age(df):
    rows_to_drop = df[df['tenure'] > df['age']].index
    df = df.drop(rows_to_drop)
    return df

def clean_vanguard_data():
    # Reading the Data files
    url = "https://raw.githubusercontent.com/data-bootcamp-v4/lessons/main/5_6_eda_inf_stats_tableau/project/files_for_project/df_final_demo.txt"
    df_final_Demo = pd.read_csv(url, on_bad_lines='skip')

    url = "https://raw.githubusercontent.com/data-bootcamp-v4/lessons/refs/heads/main/5_6_eda_inf_stats_tableau/project/files_for_project/df_final_web_data_pt_1.txt"
    df_final_web_data_pt_1 = pd.read_csv(url, on_bad_lines='skip')

    url = "https://raw.githubusercontent.com/data-bootcamp-v4/lessons/refs/heads/main/5_6_eda_inf_stats_tableau/project/files_for_project/df_final_web_data_pt_2.txt"
    df_final_web_data_pt_2 = pd.read_csv(url, on_bad_lines='skip')

    url = "https://raw.githubusercontent.com/data-bootcamp-v4/lessons/refs/heads/main/5_6_eda_inf_stats_tableau/project/files_for_project/df_final_experiment_clients.txt"
    df_final_experiment_clients = pd.read_csv(url, on_bad_lines='skip')

    # Drop duplicates
    df_final_Demo = df_final_Demo.drop_duplicates()
    df_final_web_data_pt_1 = df_final_web_data_pt_1.drop_duplicates()
    df_final_web_data_pt_2 = df_final_web_data_pt_2.drop_duplicates()
    df_final_experiment_clients = df_final_experiment_clients.drop_duplicates()

    # Drop NA
    df_final_Demo = df_final_Demo.dropna().reset_index(drop=True)

    # Combine web data
    df_final_web_data_combined = pd.concat([df_final_web_data_pt_1, df_final_web_data_pt_2], axis=0, ignore_index=True)
    df_final_web_data_combined["date_time"] = pd.to_datetime(df_final_web_data_combined["date_time"])

    # Clean gender
    df_final_Demo["gendr"] = df_final_Demo["gendr"].replace(["X"], "Unknown")
    df_final_Demo['gendr'] = df_final_Demo['gendr'].replace('U', 'Unknown')

    # Drop visitor_id
    df_final_web_data_combined = df_final_web_data_combined.drop(columns=['visitor_id'])

    # Merge
    df_final_merged = pd.merge(df_final_Demo, df_final_web_data_combined, on='client_id', how='inner')
    df_exp = df_final_merged.merge(df_final_experiment_clients, on='client_id', how='inner')

    return df_exp

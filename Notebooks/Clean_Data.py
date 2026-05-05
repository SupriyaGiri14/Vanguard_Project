import pandas as pd

# Clean All rows which tenure above age
def clean_rows_tenure_above_age(df):
    rows_to_drop = df[df['tenure_years'] > df['age']].index
    df = df.drop(rows_to_drop)
    return df

def get_cleaned_vanguard_data():
    # Reading the Data files
    url = "https://raw.githubusercontent.com/data-bootcamp-v4/lessons/main/5_6_eda_inf_stats_tableau/project/files_for_project/df_final_demo.txt"
    df_final_Demo = pd.read_csv(url, on_bad_lines='skip')

    url = "https://raw.githubusercontent.com/data-bootcamp-v4/lessons/refs/heads/main/5_6_eda_inf_stats_tableau/project/files_for_project/df_final_web_data_pt_1.txt"
    df_final_web_data_pt_1 = pd.read_csv(url, on_bad_lines='skip')

    url = "https://raw.githubusercontent.com/data-bootcamp-v4/lessons/refs/heads/main/5_6_eda_inf_stats_tableau/project/files_for_project/df_final_web_data_pt_2.txt"
    df_final_web_data_pt_2 = pd.read_csv(url, on_bad_lines='skip')

    url = "https://raw.githubusercontent.com/data-bootcamp-v4/lessons/refs/heads/main/5_6_eda_inf_stats_tableau/project/files_for_project/df_final_experiment_clients.txt"
    df_final_experiment_clients = pd.read_csv(url, on_bad_lines='skip')

    df_final_Demo = df_final_Demo.rename(columns={
    'client_id': 'client_id',
    'clnt_tenure_yr': 'tenure_years',
    'clnt_tenure_mnth': 'tenure_months',
    'clnt_age': 'age',
    'gendr': 'gender',
    'num_accts': 'num_accounts',
    'bal': 'account_balance',
    'calls_6_mnth': 'calls_last_6m',
    'logons_6_mnth': 'logins_last_6m'
    })

    df_final_experiment_clients = df_final_experiment_clients.rename(columns={
    'client_id': 'client_id',
    'Variation': 'variation'
    })

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
    df_final_Demo["gender"] = df_final_Demo["gender"].replace(["X"], "Unknown")
    df_final_Demo['gender'] = df_final_Demo['gender'].replace('U', 'Unknown')

    # Drop visitor_id
    df_final_web_data_combined = df_final_web_data_combined.drop(columns=['visitor_id'])

    # Merge
    df_final_merged = pd.merge(df_final_Demo, df_final_web_data_combined, on='client_id', how='inner')
    df_final_merged = df_final_merged.merge(df_final_experiment_clients, on='client_id', how='inner')
    df_final_merged = clean_rows_tenure_above_age(df_final_merged)

    return create_time_aware_behavior_report(df_final_merged)

def create_time_aware_behavior_report(df):
    
    # 1. Sort chronologically so we see the journey exactly as it happened.
    df['date_time'] = pd.to_datetime(df['date_time'])
    df = df.sort_values(by=['client_id', 'date_time'])
    
    # Mapping steps to numbers to detect forward/backward movement
    step_map = {'start': 0, 'step_1': 1, 'step_2': 2, 'step_3': 3, 'confirm': 4}
    ideal_sequence = ['start', 'step_1', 'step_2', 'step_3', 'confirm']

    def classify_journey(group):
        steps = group['process_step'].tolist()
        step_nums = [step_map.get(s, -1) for s in steps]
        
        if steps == ideal_sequence:
            return 'perfect_path'
        
        for i in range(1, len(step_nums)):
            if step_nums[i] < step_nums[i-1]:
                return 'confused'
        
        if 'confirm' in steps:
            for i in range(1, len(step_nums)):
                if step_nums[i] > step_nums[i-1] + 1:
                    return 'skipped_steps'
            
            if steps[0] != 'start':
                return 'skipped_steps'
                
            return 'successful_with_repeats'
            
        return 'dropped_in_between'

    # 2. Apply the logic to each unique visit
    print("Analyzing chronological journeys...")
    # Using .apply(include_groups=False) if you are on a newer pandas version to avoid warnings
    classified_clients = df.groupby(['client_id']).apply(classify_journey).reset_index()
    classified_clients.columns = ['client_id', 'path_category']

    # 3. Priority Aggregation
    priority = {
        'perfect_path': 1,
        'successful_with_repeats': 2,
        'skipped_steps': 3,
        'confused': 4,
        'dropped_in_between': 5
    }
    classified_clients['priority'] = classified_clients['path_category'].map(priority)
    
    final_behavior = (classified_clients.sort_values('priority')
                .groupby('client_id')
                .first()
                .reset_index())

    # ✅ NEW: Extract Demographic & Experiment info per client
    # We take the 'first' non-null value for each attribute
    client_info = (
        df.groupby('client_id')
        .agg({
            'variation': 'first',
            'age': 'first',
            'gender':'first',
            'tenure_years': 'first',
            'account_balance': 'first'
        })
        .reset_index()
    )

    # 4. Merge behavior with client info
    client_level_df = final_behavior.merge(client_info, on='client_id', how='left')

    # Drop rows where Variation is still None after the merge
    client_level_df = client_level_df.dropna(subset=['variation'])
    
    return client_level_df[['client_id', 'path_category', 'variation', 'age', 'gender','tenure_years', 'account_balance']]
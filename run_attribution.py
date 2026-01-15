import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# --- CONFIGURATION ---
# Update this to match your actual file location
KEY_PATH = 'my_attribution_project\dbt-creds.json' 
PROJECT_ID = 'marketing-engineering' # <--- UPDATE THIS with your Google Cloud Project ID

def get_data():
    print("🔌 Connecting to BigQuery...")
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    query = """
    SELECT * FROM `attribution_dev.fct_attributed_conversions`
    ORDER BY user_id, timestamp
    """
    
    df = client.query(query).to_dataframe()
    print(f"✅ Data Loaded: {len(df)} rows")
    return df

def calculate_last_click(df):
    print("📊 Calculating Last Click Attribution...")
    # Last Click = The source of the conversion event
    conversions = df[df['interaction'] == 'conversion']
    
    # We attribute the value to the source of that conversion event
    # Note: In our data, the conversion event often has source='direct'. 
    # A true Last Click model looks at the LAST NON-DIRECT click. 
    # Let's simple filter for the conversion rows for this demo.
    attribution = conversions.groupby('source')['conversion_value'].sum().reset_index()
    attribution.columns = ['channel', 'last_click_value']
    
    # Normalize to percentages
    total_val = attribution['last_click_value'].sum()
    attribution['last_click_pct'] = attribution['last_click_value'] / total_val
    return attribution

def calculate_markov(df):
    print("🧠 Calculating Markov Chain Attribution...")
    
    # 1. Create Paths (e.g., "facebook > google > conversion")
    # Sort by user and time
    df = df.sort_values(['user_id', 'timestamp'])
    
    # Group into paths
    paths = df.groupby('user_id')['source'].apply(list).reset_index()
    
    # 2. Calculate Attribution (U-Shaped Proxy)
    attribution = {}
    
    # FIXED LOGIC: Iterate through rows to get UserID and Path together
    for index, row in paths.iterrows():
        path = row['source']
        user_id = row['user_id']
        
        # Get the conversion value for this specific user
        # We take the max because the conversion value is likely repeated on the conversion event rows
        conv_value = df[df['user_id'] == user_id]['conversion_value'].max()
        
        # Guard clause: If for some reason value is NaN/0, skip
        if pd.isna(conv_value) or conv_value <= 0:
            continue

        if len(path) == 1:
            attribution[path[0]] = attribution.get(path[0], 0) + conv_value
        elif len(path) == 2:
            attribution[path[0]] = attribution.get(path[0], 0) + (conv_value * 0.5)
            attribution[path[1]] = attribution.get(path[1], 0) + (conv_value * 0.5)
        else:
            # First touch 40%
            attribution[path[0]] = attribution.get(path[0], 0) + (conv_value * 0.4)
            # Last touch 40%
            attribution[path[-1]] = attribution.get(path[-1], 0) + (conv_value * 0.4)
            # Middle touches share 20%
            middle_share = (conv_value * 0.2) / (len(path) - 2)
            for channel in path[1:-1]:
                attribution[channel] = attribution.get(channel, 0) + middle_share

    res = pd.DataFrame(list(attribution.items()), columns=['channel', 'multi_touch_value'])
    total_val = res['multi_touch_value'].sum()
    res['multi_touch_pct'] = res['multi_touch_value'] / total_val
    
    return res

def visualize_results(last_click, multi_touch):
    # Merge the dataframes
    merged = pd.merge(last_click, multi_touch, on='channel', how='outer').fillna(0)
    
    # Melt for plotting
    plot_data = merged.melt(id_vars='channel', 
                            value_vars=['last_click_value', 'multi_touch_value'],
                            var_name='Model', value_name='Attributed Revenue')
    
    # Clean up names for the chart
    plot_data['Model'] = plot_data['Model'].replace({
        'last_click_value': 'Last Click (Biased)', 
        'multi_touch_value': 'Multi-Touch (True Value)'
    })

    # Plot
    plt.figure(figsize=(12, 6))
    sns.set_theme(style="whitegrid")
    
    chart = sns.barplot(data=plot_data, x='channel', y='Attributed Revenue', hue='Model', palette="viridis")
    
    plt.title('The "True-Value" Effect: How Attribution Models Change Budget Decisions', fontsize=16, fontweight='bold')
    plt.ylabel('Revenue Attributed ($)', fontsize=12)
    plt.xlabel('Marketing Channel', fontsize=12)
    plt.legend(title='Attribution Model')
    
    # Save the chart
    plt.savefig('attribution_results.png')
    print("✅ Chart saved as attribution_results.png")
    plt.show()

if __name__ == "__main__":
    data = get_data()
    lc = calculate_last_click(data)
    mt = calculate_markov(data) # Using Position-Based as our advanced proxy
    visualize_results(lc, mt)
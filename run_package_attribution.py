import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.oauth2 import service_account
from collections import defaultdict
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# --- CONFIGURATION ---
KEY_PATH = 'my_attribution_project\dbt-creds.json' 
PROJECT_ID = 'marketing-engineering' # <--- UPDATE THIS

# --- CUSTOM MARKOV ENGINE ---
class RobustMarkovModel:
    """
    A custom Marketing Engineering class to calculate the Removal Effect.
    Replaces broken off-the-shelf packages.
    """
    def __init__(self, df):
        self.df = df.copy()
        self.transitions = defaultdict(int)
        self.conversion_rates = {}
        self.removal_effects = {}
        
    def fit(self):
        print("   ...Building Transition Matrix")
        # 1. Sort and create paths
        self.df = self.df.sort_values(['cookie', 'time'])
        paths = self.df.groupby('cookie')['channel'].apply(list).reset_index()
        
        # 2. Count transitions (A -> B)
        for path in paths['channel']:
            # Add 'Start' node
            unique_channels = ['(start)'] + path
            
            # Check if this user eventually converted
            user_id = paths[paths['channel'].apply(lambda x: x == path)]['cookie'].values[0]
            # Check the raw df for conversion flag
            has_converted = self.df[self.df['cookie'] == user_id]['conversion'].max()
            
            if has_converted:
                unique_channels.append('(conversion)')
            else:
                unique_channels.append('(null)')
                
            for i in range(len(unique_channels)-1):
                from_node = unique_channels[i]
                to_node = unique_channels[i+1]
                self.transitions[(from_node, to_node)] += 1
                
        # 3. Calculate Probabilities
        self.transition_probs = {}
        outbound_counts = defaultdict(int)
        
        for (from_node, _), count in self.transitions.items():
            outbound_counts[from_node] += count
            
        for (from_node, to_node), count in self.transitions.items():
            probability = count / outbound_counts[from_node]
            self.transition_probs[(from_node, to_node)] = probability
            
        return self

    def calculate_attribution(self):
        print("   ...Calculating Removal Effects (This determines true value)")
        
        # 1. Find all unique channels (excluding start/null/conversion)
        all_channels = set([k[0] for k in self.transitions.keys() if k[0] not in ['(start)', '(null)', '(conversion)']])
        
        # 2. Calculate Base Conversion Probability (Total System)
        base_conversion = self._calculate_conversion_probability(self.transition_probs)
        
        # 3. Calculate Removal Effect for each channel
        # "How much does conversion drop if we delete Facebook?"
        results = {}
        
        for channel in all_channels:
            # Create a matrix where this channel acts as a 'dead end' (100% to null)
            temp_probs = self.transition_probs.copy()
            
            # Redirect all traffic FROM this channel to (null)
            # And remove any traffic GOING TO this channel (effectively skipping it)
            # Simplified approach: Just make the channel loop to null.
            for key in list(temp_probs.keys()):
                if key[0] == channel:
                    del temp_probs[key]
                elif key[1] == channel:
                    # Redirect incoming traffic to null? 
                    # Simpler: Make the channel a sink.
                    temp_probs[key] = 0 # Cut the link
            
            # Recalculate system conversion
            new_conversion = self._calculate_conversion_probability(temp_probs)
            
            # Removal Effect = 1 - (New / Old)
            if base_conversion > 0:
                removal_effect = 1 - (new_conversion / base_conversion)
            else:
                removal_effect = 0
                
            results[channel] = removal_effect
            
        # 4. Allocate Revenue based on Removal Effect weights
        total_removal_effect = sum(results.values())
        
        attribution = {}
        total_revenue = self.df[self.df['conversion'] == True]['conversion_value'].sum()
        
        for channel, effect in results.items():
            weight = effect / total_removal_effect if total_removal_effect > 0 else 0
            attribution[channel] = weight * total_revenue
            
        return pd.DataFrame(list(attribution.items()), columns=['Channel', 'Markov Value'])

    def _calculate_conversion_probability(self, probs_dict):
        # A simple iterative path probability calculator
        # In a full graph, this requires matrix inversion (I-Q)^-1
        # For simplicity/robustness in a demo, we simulate flow or simple prob multiplication
        # Let's use a Memoization approach to find prob from (start) to (conversion)
        
        memo = {}
        
        def get_prob(node, visited):
            if node == '(conversion)': return 1.0
            if node == '(null)': return 0.0
            if node in visited: return 0.0 # prevent loops
            if node in memo: return memo[node]
            
            visited.add(node)
            total_prob = 0
            
            # Find all neighbors
            neighbors = [k[1] for k in probs_dict.keys() if k[0] == node]
            
            for neighbor in neighbors:
                p = probs_dict.get((node, neighbor), 0)
                if p > 0:
                    total_prob += p * get_prob(neighbor, visited.copy())
            
            memo[node] = total_prob
            return total_prob
            
        return get_prob('(start)', set())

# --- MAIN PIPELINE ---
def get_data_and_format():
    print("🔌 Connecting to BigQuery...")
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    query = """
    SELECT * FROM `attribution_dev.fct_attributed_conversions`
    ORDER BY user_id, timestamp
    """
    
    df = client.query(query).to_dataframe()
    
    print("🧹 Pre-processing data...")
    # Standardize column names
    df = df.rename(columns={
        'user_id': 'cookie',
        'timestamp': 'time',
        'source': 'channel',
        'interaction': 'interaction' 
    })
    
    # Ensure boolean
    df['conversion'] = df['interaction'] == 'conversion'
    df['conversion_value'] = df['conversion_value'].fillna(0)
    
    return df

def run_comparison(df):
    print("🧮 Running Attribution Models...")
    
    # 1. Custom Markov Model
    model = RobustMarkovModel(df)
    model.fit()
    markov_df = model.calculate_attribution()
    
    # 2. Last Click (Simple Benchmark)
    print("   ...Calculating Last Click")
    lc_df = df[df['conversion'] == True].groupby('channel')['conversion_value'].sum().reset_index()
    lc_df.columns = ['Channel', 'Last Click Value']
    
    # 3. Merge
    comparison = pd.merge(markov_df, lc_df, on='Channel', how='outer').fillna(0)
    return comparison

def visualize(comparison_df):
    print("🎨 Plotting comparison...")
    
    plot_data = comparison_df.melt(id_vars='Channel', 
                                   value_vars=['Markov Value', 'Last Click Value'],
                                   var_name='Model', value_name='Attributed Revenue')
    
    plt.figure(figsize=(14, 7))
    sns.set_theme(style="whitegrid")
    
    sns.barplot(data=plot_data, x='Channel', y='Attributed Revenue', hue='Model', palette="magma")
    
    plt.title('Attribution Engine Results: Markov Chain (Removal Effect) vs. Last Click', fontsize=16, fontweight='bold')
    plt.ylabel('Attributed Revenue ($)', fontsize=12)
    plt.xlabel('Marketing Channel', fontsize=12)
    plt.xticks(rotation=45)
    plt.legend(title='Attribution Model')
    
    plt.tight_layout()
    plt.savefig('final_attribution_comparison.png')
    print("✅ SUCCESS: Saved 'final_attribution_comparison.png'")
    plt.show()

if __name__ == "__main__":
    df = get_data_and_format()
    results = run_comparison(df)
    visualize(results)
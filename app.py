import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from collections import defaultdict

# --- PAGE CONFIG ---
st.set_page_config(page_title="Attribution Engine", layout="wide")

# --- CSS FOR STYLING ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .stMetricLabel {
        font-weight: bold;
        color: #555;
    }
</style>
""", unsafe_allow_html=True)

# --- CONFIGURATION ---
KEY_PATH = 'my_attribution_project/dbt-creds.json' 
PROJECT_ID = 'marketing-attribution' # <--- UPDATE THIS TO YOUR ID

# --- CUSTOM MARKOV CLASS (Embedded for Portability) ---
class RobustMarkovModel:
    def __init__(self, df):
        self.df = df.copy()
        self.transitions = defaultdict(int)
        self.conversion_rates = {}
        self.removal_effects = {}
        
    def fit(self):
        self.df = self.df.sort_values(['cookie', 'time'])
        paths = self.df.groupby('cookie')['channel'].apply(list).reset_index()
        
        for path in paths['channel']:
            unique_channels = ['(start)'] + path
            user_id = paths[paths['channel'].apply(lambda x: x == path)]['cookie'].values[0]
            has_converted = self.df[self.df['cookie'] == user_id]['conversion'].max()
            
            if has_converted:
                unique_channels.append('(conversion)')
            else:
                unique_channels.append('(null)')
                
            for i in range(len(unique_channels)-1):
                self.transitions[(unique_channels[i], unique_channels[i+1])] += 1
                
        # Calculate Probabilities
        self.transition_probs = {}
        outbound_counts = defaultdict(int)
        for (from_node, _), count in self.transitions.items():
            outbound_counts[from_node] += count
            
        for (from_node, to_node), count in self.transitions.items():
            self.transition_probs[(from_node, to_node)] = count / outbound_counts[from_node]
        return self

    def calculate_attribution(self):
        # Base Conversion
        base_conversion = self._calculate_conversion_probability(self.transition_probs)
        
        # Removal Effects
        all_channels = set([k[0] for k in self.transitions.keys() if k[0] not in ['(start)', '(null)', '(conversion)']])
        results = {}
        
        for channel in all_channels:
            temp_probs = self.transition_probs.copy()
            for key in list(temp_probs.keys()):
                if key[0] == channel: del temp_probs[key]
                elif key[1] == channel: temp_probs[key] = 0
            
            new_conversion = self._calculate_conversion_probability(temp_probs)
            results[channel] = 1 - (new_conversion / base_conversion) if base_conversion > 0 else 0
            
        # Allocate Revenue
        total_removal_effect = sum(results.values())
        attribution = {}
        total_revenue = self.df[self.df['conversion'] == True]['conversion_value'].sum()
        
        for channel, effect in results.items():
            weight = effect / total_removal_effect if total_removal_effect > 0 else 0
            attribution[channel] = weight * total_revenue
            
        return pd.DataFrame(list(attribution.items()), columns=['Channel', 'Markov Value'])

    def _calculate_conversion_probability(self, probs_dict):
        memo = {}
        def get_prob(node, visited):
            if node == '(conversion)': return 1.0
            if node == '(null)': return 0.0
            if node in visited: return 0.0
            if node in memo: return memo[node]
            visited.add(node)
            total_prob = 0
            neighbors = [k[1] for k in probs_dict.keys() if k[0] == node]
            for neighbor in neighbors:
                p = probs_dict.get((node, neighbor), 0)
                if p > 0: total_prob += p * get_prob(neighbor, visited.copy())
            memo[node] = total_prob
            return total_prob
        return get_prob('(start)', set())

# --- DATA LOADER ---
@st.cache_data
def load_data():
    try:
        credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        query = "SELECT * FROM `attribution_dev.fct_attributed_conversions` ORDER BY user_id, timestamp"
        df = client.query(query).to_dataframe()
        
        # Preprocessing
        df = df.rename(columns={'user_id': 'cookie', 'timestamp': 'time', 'source': 'channel'})
        df['conversion'] = df['interaction'] == 'conversion'
        df['conversion_value'] = df['conversion_value'].fillna(0)
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# --- MAIN APP UI ---
def main():
    st.title("🚀 True-Value Attribution Engine")
    st.markdown("Comparing **Last-Click Bias** vs. **Algorithmic Reality** (Markov Chain)")
    
    # 1. Load Data
    with st.spinner('Querying BigQuery...'):
        df = load_data()
        
    if df.empty:
        return

    # 2. Sidebar Filters
    st.sidebar.header("Filter Settings")
    all_channels = df['channel'].unique()
    selected_channels = st.sidebar.multiselect("Select Channels to Analyze", all_channels, default=all_channels)
    
    # Filter Logic
    filtered_df = df[df['channel'].isin(selected_channels)]
    
    # 3. Run Models Live
    model = RobustMarkovModel(filtered_df)
    model.fit()
    markov_df = model.calculate_attribution()
    
    lc_df = filtered_df[filtered_df['conversion'] == True].groupby('channel')['conversion_value'].sum().reset_index()
    lc_df.columns = ['Channel', 'Last Click Value']
    
    # Merge
    comparison = pd.merge(markov_df, lc_df, on='Channel', how='outer').fillna(0)
    comparison['Lift'] = ((comparison['Markov Value'] - comparison['Last Click Value']) / comparison['Last Click Value']) * 100
    
    # 4. Top Level Metrics
    total_rev = comparison['Markov Value'].sum()
    st.markdown("### 📊 Executive Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Attributed Revenue", f"${total_rev:,.2f}")
    col2.metric("Channels Analyzed", len(comparison))
    col3.metric("Model Type", "Markov Chain (Removal Effect)")
    
    st.markdown("---")

    # 5. Interactive Charts
    col_chart, col_data = st.columns([2, 1])
    
    with col_chart:
        st.subheader("Attribution Model Comparison")
        # Plotly Bar Chart
        plot_data = comparison.melt(id_vars='Channel', value_vars=['Markov Value', 'Last Click Value'], var_name='Model', value_name='Revenue')
        fig = px.bar(plot_data, x='Channel', y='Revenue', color='Model', barmode='group',
                     color_discrete_map={'Markov Value': '#636EFA', 'Last Click Value': '#EF553B'})
        fig.update_layout(xaxis_title="", yaxis_title="Revenue ($)", legend_title="", template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

    with col_data:
        st.subheader("ROI Lift (Truth vs. Lie)")
        # Show which channels are being ignored
        comparison['Status'] = comparison['Lift'].apply(lambda x: 'Undervalued 🟢' if x > 0 else 'Overvalued 🔴')
        st.dataframe(comparison[['Channel', 'Lift', 'Status']].style.format({'Lift': "{:.1f}%"}), hide_index=True)

    # 6. Detailed Data Table
    st.markdown("### 📝 Detailed Breakdown")
    st.dataframe(comparison.style.format({'Markov Value': "${:,.2f}", 'Last Click Value': "${:,.2f}", 'Lift': "{:.1f}%"}), use_container_width=True)

if __name__ == "__main__":
    main()
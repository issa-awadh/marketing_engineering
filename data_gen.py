import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Settings
NUM_USERS = 5000
START_DATE = datetime(2025, 1, 1)
DAYS = 60

def generate_messy_data():
    print("Generating synthetic marketing data...")
    
    # --- 1. Generate Ad Spend Data (with intentional messiness) ---
    platforms = ['Facebook', 'Google Ads', 'TikTok', 'Email_Newsletter']
    # Intentional inconsistency in naming to fix later in dbt
    campaigns = {
        'Facebook': ['Summer_Sale', 'summer sale', 'Retargeting_GenZ', 'Brand_Awareness'],
        'Google Ads': ['Search_Brand', 'search_generic', 'Display_Retargeting'],
        'TikTok': ['Viral_Video_1', 'Influencer_Collab'],
        'Email_Newsletter': ['Weekly_Digest', 'Welcome_Sequence']
    }
    
    spend_data = []
    current_date = START_DATE
    
    for _ in range(DAYS):
        for platform in platforms:
            # Randomize daily spend
            daily_spend = random.uniform(50, 500)
            
            # Pick a random campaign (including messy variants)
            camp_list = campaigns[platform]
            campaign_name = random.choice(camp_list)
            
            # 10% chance of a data entry error (NULL campaign name or outlier spend)
            if random.random() < 0.1:
                campaign_name = None 
            
            spend_data.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'platform': platform,
                'campaign_name': campaign_name,
                'cost': round(daily_spend, 2)
            })
        current_date += timedelta(days=1)
        
    df_spend = pd.DataFrame(spend_data)
    df_spend.to_csv('ad_spend.csv', index=False)
    print(f"✅ Generated ad_spend.csv ({len(df_spend)} rows)")

    # --- 2. Generate User Journey Data (Touchpoints) ---
    user_data = []
    
    for user_id in range(1, NUM_USERS + 1):
        # Determine how many touchpoints this user has (1 to 8)
        num_touches = np.random.choice(range(1, 9), p=[0.3, 0.25, 0.2, 0.1, 0.05, 0.05, 0.03, 0.02])
        
        user_start_time = START_DATE + timedelta(days=random.randint(0, DAYS-5))
        current_time = user_start_time
        
        has_converted = 0
        conversion_value = 0
        
        # Journey Loop
        for i in range(num_touches):
            platform = random.choice(platforms)
            source = platform
            medium = 'cpc' if platform in ['Facebook', 'Google Ads'] else 'social'
            
            # Intentional messiness: Inconsistent UTM tagging
            if random.random() < 0.05:
                source = source.lower() # lowercase inconsistency
            
            user_data.append({
                'user_id': user_id,
                'timestamp': current_time,
                'source': source,
                'medium': medium,
                'campaign': random.choice(campaigns[platform]) if platform in campaigns else 'unknown',
                'interaction': 'click'
            })
            
            # Time gap between touches (hours or days)
            time_gap = timedelta(hours=random.randint(1, 48))
            current_time += time_gap
            
        # Determine Conversion (Last touch)
        # 15% conversion rate overall
        if random.random() < 0.15:
            has_converted = 1
            conversion_value = random.uniform(50, 200)
            
            # Append the conversion event
            user_data.append({
                'user_id': user_id,
                'timestamp': current_time + timedelta(minutes=5),
                'source': 'direct', # Conversion usually happens on site
                'medium': 'none',
                'campaign': None,
                'interaction': 'conversion',
                'conversion_value': round(conversion_value, 2)
            })

    df_journey = pd.DataFrame(user_data)
    
    # Save to CSV
    df_journey.to_csv('user_journeys.csv', index=False)
    print(f"✅ Generated user_journeys.csv ({len(df_journey)} rows)")

if __name__ == "__main__":
    generate_messy_data()
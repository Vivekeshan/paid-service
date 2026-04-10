import pandas as pd
import json
from datetime import datetime

def generate_service_predictions(file_path):
    print("Loading data...")
    # 1. Load Data
    df = pd.read_excel(file_path)
    
    # Force invalid dates (like "0/1") to become 'NaT' (Not a Time)
    df['JOB CARD DATE'] = pd.to_datetime(df['JOB CARD DATE'], errors='coerce')
    df['DT. OF DELIVERY'] = pd.to_datetime(df['DT. OF DELIVERY'], errors='coerce')
    
    # Drop any rows where the dates were so corrupted they became 'NaT'
    # Also drop rows where REG NUMBER is missing
    df = df.dropna(subset=['JOB CARD DATE', 'DT. OF DELIVERY', 'REG NUMBER'])
    
    today = datetime.now()
    results = []

    print(f"Processing {len(df['REG NUMBER'].unique())} unique vehicles...")

    # 2. Group by Registration Number
    for reg, group in df.groupby('REG NUMBER'):
        group = group.sort_values('JOB CARD DATE', ascending=False)
        latest = group.iloc[0]
        
        # Calculate Velocity (Synthetic Mileage)
        service_count = len(group)
        days_since_last = (today - latest['DT. OF DELIVERY']).days
        
        # Prevent negative days if there's a future date error in the Excel
        if days_since_last < 0:
            days_since_last = 0
            
        if service_count > 1:
            # Calculate actual velocity from history
            prev = group.iloc[1]
            delta_days = (latest['JOB CARD DATE'] - prev['JOB CARD DATE']).days
            velocity = 10000 / max(delta_days, 1) # Avg 10k km per service interval
        else:
            # Fallback for first-timers
            velocity = 40 if "NEXA" in str(latest['CAR MODEL']).upper() else 30

        # Predict KM
        predicted_km = days_since_last * velocity
        
        # Scoring Logic
        score = 0
        if (predicted_km % 10000 < 500) or (predicted_km % 10000 > 9500):
            score += 0.50  # Interval Bonus
        if days_since_last > 360:
            score += 0.30  # Retention Bonus
        if service_count > 3:
            score += 0.20  # Loyalty Bonus

        # Extract Real Customer Info Safely
        name = str(latest['NAME OF CUSTOMER']) if pd.notna(latest['NAME OF CUSTOMER']) else "Unknown"
        phone = str(latest['PHONE NUMBER']) if pd.notna(latest['PHONE NUMBER']) else "N/A"
        model = str(latest['CAR MODEL']) if pd.notna(latest['CAR MODEL']) else "Unknown"

        # Fix Excel formatting phone numbers as floats (e.g., 9876543210.0 -> 9876543210)
        if phone.endswith(".0"):
            phone = phone[:-2]

        results.append({
            "REG_NO": str(reg),
            "Name": name,
            "Phone": phone,
            "Model": model,
            "Days": int(days_since_last),
            "Service_Count": int(service_count),
            "Probability": round(min(score, 0.98), 2) # Cap at 98% for realism
        })

    # SPEED OPTIMIZATION: Filter out cold leads (below 30%) to keep the web dashboard fast
    results = [r for r in results if r["Probability"] >= 0.30]

    # 3. Save to output.json for the Portal
    print("Saving output.json...")
    with open('output.json', 'w') as f:
        json.dump(results, f)
        
    print(f"Success! Exported {len(results)} high-priority leads.")

# Run the engine
if __name__ == "__main__":
    generate_service_predictions('service_dump.xlsx')